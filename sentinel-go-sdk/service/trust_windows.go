//go:build windows

package service

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// trustedSIDs is SYSTEM, Administrators, TrustedInstaller and the account this process runs as.
func trustedSIDs() ([]string, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, fmt.Errorf("reading process user: %w", err)
	}

	return []string{sidLocalSystem, sidAdministrators, sidTrustedInstaller, user.User.Sid.String()}, nil
}

func checkEntry(path string, kind entryKind) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("checking %s: %w", path, err)
	}

	if kind == entryFile && !info.Mode().IsRegular() {
		return fmt.Errorf("%w: %s is not a regular file", ErrUntrustedBinary, path)
	}

	trusted, err := trustedSIDs()
	if err != nil {
		return err
	}

	sd, err := windows.GetNamedSecurityInfo(
		path, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("reading security of %s: %w", path, err)
	}

	owner, _, err := sd.Owner()
	if err != nil {
		return fmt.Errorf("reading owner of %s: %w", path, err)
	}

	dacl, _, err := sd.DACL()
	if err != nil && err != windows.ERROR_OBJECT_NOT_FOUND { //nolint:errorlint // sentinel from DACL()
		return fmt.Errorf("reading access list of %s: %w", path, err)
	}

	aces, err := readACEs(dacl)
	if err != nil {
		return fmt.Errorf("reading access list of %s: %w", path, err)
	}

	if !windowsEntryTrusted(owner.String(), aces, dacl != nil, trusted, kind) {
		return fmt.Errorf("%w: %s (owner %s)", ErrUntrustedBinary, path, owner.String())
	}

	return nil
}

func readACEs(acl *windows.ACL) ([]windowsACE, error) {
	if acl == nil {
		return nil, nil
	}

	aces := make([]windowsACE, 0, acl.AceCount)

	for i := range uint32(acl.AceCount) {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(acl, i, &ace); err != nil {
			return nil, fmt.Errorf("entry %d: %w", i, err)
		}

		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
			continue
		}

		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart)) //#nosec G103 -- SID follows the ACE header

		aces = append(aces, windowsACE{
			inheritOnly: ace.Header.AceFlags&windows.INHERIT_ONLY_ACE != 0,
			mask:        uint32(ace.Mask),
			sid:         sid.String(),
		})
	}

	return aces, nil
}
