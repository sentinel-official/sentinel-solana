package wgcore

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"net/netip"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

const (
	keyBytes         = 32
	maxPeerAddresses = 8
	minMTU           = 1280
)

type Metadata[E service.Validator] struct {
	Extensions E      `json:"extensions,omitzero"`
	Port       uint16 `json:"port"`
	PublicKey  string `json:"public_key"`
}

func (m Metadata[E]) Validate() error {
	if err := validatePort(m.Port); err != nil {
		return fmt.Errorf("%w: %w", validate.ErrInvalidConfig, err)
	}

	if _, err := DecodeKey(m.PublicKey); err != nil {
		return fmt.Errorf("%w: public key: %w", validate.ErrInvalidConfig, err)
	}

	if err := m.Extensions.Validate(); err != nil {
		return fmt.Errorf("extensions: %w", err)
	}

	return nil
}

type PeerRequest struct {
	PublicKey string `json:"public_key"`
}

func (r PeerRequest) Validate() error {
	if _, err := DecodeKey(r.PublicKey); err != nil {
		return fmt.Errorf("%w: public key: %w", service.ErrInvalidPeer, err)
	}

	return nil
}

type PeerResponse[E service.Validator] struct {
	Addresses  []string `json:"addresses"`
	Extensions E        `json:"extensions,omitzero"`
	MTU        uint16   `json:"mtu"`
	Port       uint16   `json:"port"`
	PublicKey  string   `json:"public_key"`
}

func (r PeerResponse[E]) Validate() error {
	if err := validateAddresses(r.Addresses); err != nil {
		return fmt.Errorf("%w: addresses: %w", service.ErrInvalidPeer, err)
	}

	if r.MTU < minMTU {
		return fmt.Errorf("%w: mtu %d below %d", service.ErrInvalidPeer, r.MTU, minMTU)
	}

	if err := validatePort(r.Port); err != nil {
		return fmt.Errorf("%w: %w", service.ErrInvalidPeer, err)
	}

	if _, err := DecodeKey(r.PublicKey); err != nil {
		return fmt.Errorf("%w: public key: %w", service.ErrInvalidPeer, err)
	}

	if err := r.Extensions.Validate(); err != nil {
		return fmt.Errorf("%w: extensions: %w", service.ErrInvalidPeer, err)
	}

	return nil
}

type ServerConfig[E service.Validator] struct {
	Addresses       []netip.Prefix
	EgressInterface string
	Extensions      E
	ListenPort      uint16
	MTU             uint16
	PrivateKey      string
}

type Variant struct {
	Binary              string
	Confinable          bool
	Name                service.Name
	RunDir              string
	Templates           fs.FS
	TunnelBinary        string
	TunnelPrefix        []string
	TunnelServicePrefix string
	UserspaceBinary     string
}

func DecodeKey(s string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decoding key: %w", err)
	}

	if len(raw) != keyBytes {
		return nil, fmt.Errorf("key is %d bytes, want %d", len(raw), keyBytes)
	}

	if base64.StdEncoding.EncodeToString(raw) != s {
		return nil, errors.New("key is not canonical base64")
	}

	return raw, nil
}

func DerivePublicKey(privateKey string) (string, error) {
	raw, err := DecodeKey(privateKey)
	if err != nil {
		return "", err
	}

	key, err := ecdh.X25519().NewPrivateKey(raw)
	if err != nil {
		return "", fmt.Errorf("parsing key: %w", err)
	}

	return base64.StdEncoding.EncodeToString(key.PublicKey().Bytes()), nil
}

func NewPrivateKey() (string, error) {
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return "", fmt.Errorf("generating key: %w", err)
	}

	return base64.StdEncoding.EncodeToString(key.Bytes()), nil
}

func PreflightUserspace(v Variant, goos string) []service.PreflightCheck {
	if goos != "darwin" {
		return nil
	}

	if _, err := service.LookPath(v.UserspaceBinary); err == nil {
		return nil
	}

	return []service.PreflightCheck{
		service.NewPreflightCheck(
			"service_userspace",
			service.SeverityWarning,
			v.UserspaceBinary+" not found on PATH; wg-quick on macOS needs it",
		),
	}
}

func validateAddresses(addresses []string) error {
	if len(addresses) == 0 || len(addresses) > maxPeerAddresses {
		return fmt.Errorf("%d entries, want 1 to %d", len(addresses), maxPeerAddresses)
	}

	seen := make(map[netip.Addr]bool, len(addresses))

	for _, s := range addresses {
		prefix, err := netip.ParsePrefix(s)
		if err != nil {
			return fmt.Errorf("parsing address: %w", err)
		}

		if prefix.Bits() != prefix.Addr().BitLen() {
			return fmt.Errorf("%s is not a single address", prefix)
		}

		addr := prefix.Addr().Unmap()
		if addr.IsUnspecified() || addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsMulticast() {
			return fmt.Errorf("%s is not a unicast tunnel address", prefix)
		}

		if seen[addr] {
			return fmt.Errorf("duplicate %s", prefix)
		}

		seen[addr] = true
	}

	return nil
}

func validatePort(port uint16) error {
	if port == 0 {
		return errors.New("port 0")
	}

	return nil
}
