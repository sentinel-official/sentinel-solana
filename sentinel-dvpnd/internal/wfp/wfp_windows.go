package wfp

import (
	"errors"
	"fmt"
	"net"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	ErrFilterNotFound     syscall.Errno = 0x80320003
	ErrSublayerNotFound   syscall.Errno = 0x80320007
	ErrTransactionMissing syscall.Errno = 0x8032000D
)

const (
	enumFlagIncludeDisabled uint32 = 0x10
	filterFlagDisabled      uint32 = 0x20
)

var ErrInvalidSublayer = errors.New("incompatible existing sublayer")

//nolint:gochecknoglobals
var (
	fwpuclnt = windows.NewLazySystemDLL("fwpuclnt.dll")
	iphlpapi = windows.NewLazySystemDLL("iphlpapi.dll")

	procConvertInterfaceIndexToLUID = iphlpapi.NewProc("ConvertInterfaceIndexToLuid")
	procEngineClose                 = fwpuclnt.NewProc("FwpmEngineClose0")
	procEngineOpen                  = fwpuclnt.NewProc("FwpmEngineOpen0")
	procFilterAdd                   = fwpuclnt.NewProc("FwpmFilterAdd0")
	procFilterCreateEnumHandle      = fwpuclnt.NewProc("FwpmFilterCreateEnumHandle0")
	procFilterDeleteByKey           = fwpuclnt.NewProc("FwpmFilterDeleteByKey0")
	procFilterDestroyEnumHandle     = fwpuclnt.NewProc("FwpmFilterDestroyEnumHandle0")
	procFilterEnum                  = fwpuclnt.NewProc("FwpmFilterEnum0")
	procFreeMemory                  = fwpuclnt.NewProc("FwpmFreeMemory0")
	procSubLayerAdd                 = fwpuclnt.NewProc("FwpmSubLayerAdd0")
	procSubLayerDeleteByKey         = fwpuclnt.NewProc("FwpmSubLayerDeleteByKey0")
	procSubLayerGetByKey            = fwpuclnt.NewProc("FwpmSubLayerGetByKey0")
	procTransactionAbort            = fwpuclnt.NewProc("FwpmTransactionAbort0")
	procTransactionBegin            = fwpuclnt.NewProc("FwpmTransactionBegin0")
	procTransactionCommit           = fwpuclnt.NewProc("FwpmTransactionCommit0")
)

type engine struct {
	handle      windows.Handle
	name        string
	sublayerKey guid
}

func (e *engine) addFilters(set *filterSet) error {
	for i := range set.filters {
		if err := call(procFilterAdd, uintptr(e.handle), uintptr(unsafe.Pointer(&set.filters[i])), 0, 0); err != nil {
			return fmt.Errorf("adding filter %d: %w", i, err)
		}
	}

	return nil
}

func (e *engine) close() error {
	if err := call(procEngineClose, uintptr(e.handle)); err != nil {
		return fmt.Errorf("closing the filtering engine: %w", err)
	}

	return nil
}

func (e *engine) deleteFilterKeys(keys []guid) error {
	if len(keys) == 0 {
		return nil
	}

	var pins runtime.Pinner
	defer pins.Unpin()

	pins.Pin(&keys[0])

	for i := range keys {
		err := call(procFilterDeleteByKey, uintptr(e.handle), uintptr(unsafe.Pointer(&keys[i])))
		if err != nil && !errors.Is(err, ErrFilterNotFound) {
			return fmt.Errorf("deleting filter %s: %w", keys[i], err)
		}
	}

	return nil
}

func (e *engine) deleteFilters() error {
	for _, layer := range []guid{layerALEAuthConnectV4, layerALEAuthConnectV6} {
		keys, err := e.filterKeys(layer, true)
		if err != nil {
			return err
		}

		if err := e.deleteFilterKeys(keys); err != nil {
			return err
		}
	}

	return nil
}

func (e *engine) deleteSublayer() error {
	var pins runtime.Pinner
	defer pins.Unpin()

	pins.Pin(&e.sublayerKey)

	err := call(procSubLayerDeleteByKey, uintptr(e.handle), uintptr(unsafe.Pointer(&e.sublayerKey)))
	if err != nil && !errors.Is(err, ErrSublayerNotFound) {
		return fmt.Errorf("deleting sublayer: %w", err)
	}

	return nil
}

func (e *engine) ensureSublayer() error {
	var found *sublayer

	var pins runtime.Pinner
	defer pins.Unpin()

	pins.Pin(&e.sublayerKey)
	pins.Pin(&found)

	defer func() {
		if found != nil {
			_, _, _ = procFreeMemory.Call(uintptr(unsafe.Pointer(&found)))
		}
	}()

	err := call(
		procSubLayerGetByKey,
		uintptr(e.handle),
		uintptr(unsafe.Pointer(&e.sublayerKey)),
		uintptr(unsafe.Pointer(&found)),
	)
	if err == nil {
		if found == nil || found.providerKey != nil || found.flags&sublayerFlagPersistent == 0 {
			return ErrInvalidSublayer
		}

		return nil
	}

	if !errors.Is(err, ErrSublayerNotFound) {
		return fmt.Errorf("querying sublayer: %w", err)
	}

	s := newSublayer(e.name)
	pins.Pin(s)
	pins.Pin(s.displayData.name)
	pins.Pin(s.displayData.description)

	if err := call(procSubLayerAdd, uintptr(e.handle), uintptr(unsafe.Pointer(s)), 0); err != nil {
		return fmt.Errorf("adding sublayer: %w", err)
	}

	return nil
}

func (e *engine) filterKeys(layer guid, includeDisabled bool) (keys []guid, err error) {
	tmpl := newEnumTemplate(layer)
	if includeDisabled {
		tmpl.flags = enumFlagIncludeDisabled
	}

	var enumHandle windows.Handle

	var pins runtime.Pinner
	defer pins.Unpin()

	pins.Pin(tmpl)
	pins.Pin(&enumHandle)

	if err := call(
		procFilterCreateEnumHandle,
		uintptr(e.handle),
		uintptr(unsafe.Pointer(tmpl)),
		uintptr(unsafe.Pointer(&enumHandle)),
	); err != nil {
		return nil, fmt.Errorf("creating filter enumeration in %s: %w", layer, err)
	}
	defer func() {
		if closeErr := call(procFilterDestroyEnumHandle, uintptr(e.handle), uintptr(enumHandle)); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing filter enumeration: %w", closeErr))
		}
	}()

	for {
		page, count, err := e.filterPage(enumHandle, includeDisabled)
		if err != nil {
			return nil, fmt.Errorf("enumerating filters in %s: %w", layer, err)
		}

		keys = append(keys, page...)
		if count < enumBatch {
			return keys, nil
		}
	}
}

func (e *engine) filterPage(enumHandle windows.Handle, includeDisabled bool) ([]guid, uint32, error) {
	var (
		count   uint32
		entries **filter
	)

	var pins runtime.Pinner
	defer pins.Unpin()

	pins.Pin(&entries)
	pins.Pin(&count)

	defer func() {
		if entries != nil {
			_, _, _ = procFreeMemory.Call(uintptr(unsafe.Pointer(&entries)))
		}
	}()

	if err := call(
		procFilterEnum,
		uintptr(e.handle),
		uintptr(enumHandle),
		uintptr(enumBatch),
		uintptr(unsafe.Pointer(&entries)),
		uintptr(unsafe.Pointer(&count)),
	); err != nil {
		return nil, 0, err
	}

	var keys []guid

	for _, f := range unsafe.Slice(entries, count) {
		if f.sublayerKey == e.sublayerKey && (includeDisabled || f.flags&filterFlagDisabled == 0) {
			keys = append(keys, f.key)
		}
	}

	return keys, count, nil
}

func (e *engine) transaction(fn func() error) (err error) {
	if err := call(procTransactionBegin, uintptr(e.handle), 0); err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	committed := false
	defer func() {
		if committed {
			return
		}

		if abortErr := call(procTransactionAbort, uintptr(e.handle)); abortErr != nil &&
			!errors.Is(abortErr, ErrTransactionMissing) {
			err = errors.Join(err, fmt.Errorf("aborting transaction: %w", abortErr))
		}
	}()

	if err := fn(); err != nil {
		return err
	}

	if err := call(procTransactionCommit, uintptr(e.handle)); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	committed = true

	return nil
}

func Apply(name string, rules []Rule) (err error) {
	set, err := compile(deriveSublayerKey(name), rules)
	if err != nil {
		return err
	}
	defer set.release()

	e, err := openEngine(name)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, e.close()) }()

	return e.transaction(func() error {
		if err := e.ensureSublayer(); err != nil {
			return err
		}

		if err := e.deleteFilters(); err != nil {
			return err
		}

		return e.addFilters(set)
	})
}

func Flush(name string) (err error) {
	e, err := openEngine(name)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, e.close()) }()

	return e.transaction(func() error {
		if err := e.deleteFilters(); err != nil {
			return err
		}

		return e.deleteSublayer()
	})
}

func InterfaceLUID(name string) (uint64, error) {
	ifi, err := net.InterfaceByName(name)
	if err != nil {
		return 0, fmt.Errorf("finding interface %s: %w", name, err)
	}

	var luid uint64

	var pins runtime.Pinner
	defer pins.Unpin()

	pins.Pin(&luid)

	if err := call(procConvertInterfaceIndexToLUID, uintptr(ifi.Index), uintptr(unsafe.Pointer(&luid))); err != nil {
		return 0, fmt.Errorf("resolving the luid of %s: %w", name, err)
	}

	return luid, nil
}

func Loaded(name string) (loaded bool, err error) {
	e, err := openEngine(name)
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, e.close()) }()

	for _, layer := range []guid{layerALEAuthConnectV4, layerALEAuthConnectV6} {
		keys, err := e.filterKeys(layer, false)
		if err != nil {
			return false, err
		}

		if len(keys) > 0 {
			return true, nil
		}
	}

	return false, nil
}

func call(proc *windows.LazyProc, args ...uintptr) error {
	r, _, _ := proc.Call(args...)
	if err := dwordResult(r); err != nil {
		return fmt.Errorf("calling %s: %w", proc.Name, err)
	}

	return nil
}

// dwordResult: the Fwpm* functions return a 32-bit DWORD. On arm64 the upper half of the return
// register is undefined, so the raw uintptr did not match ErrSublayerNotFound and a harmless
// "sublayer does not exist" was logged at error on every start; it could also turn a
// success into an error. Only the low 32 bits count.
func dwordResult(r uintptr) error {
	if code := uint32(r); code != 0 {
		return syscall.Errno(code)
	}

	return nil
}

func openEngine(name string) (*engine, error) {
	s := newSession(name)

	var handle windows.Handle

	var pins runtime.Pinner
	defer pins.Unpin()

	pins.Pin(s)
	pins.Pin(s.displayData.name)
	pins.Pin(s.displayData.description)
	pins.Pin(&handle)

	if err := call(
		procEngineOpen,
		0,
		uintptr(rpcAuthnWinNT),
		0,
		uintptr(unsafe.Pointer(s)),
		uintptr(unsafe.Pointer(&handle)),
	); err != nil {
		return nil, fmt.Errorf("opening the filtering engine: %w", err)
	}

	return &engine{
		handle:      handle,
		name:        name,
		sublayerKey: deriveSublayerKey(name),
	}, nil
}
