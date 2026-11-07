package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

var errFakePayload = errors.New("empty name")

type fakePayload struct {
	Name string `json:"name"`
}

func (p fakePayload) Validate() error {
	if p.Name == "" {
		return errFakePayload
	}

	return nil
}

func TestCleanupServer(t *testing.T) {
	t.Parallel()

	var cleaned []service.Placement

	hooked := service.ServerDriver[fakeConfig]{
		Bit: 1 << 15,
		Cleanup: func(_ context.Context, p service.Placement) error {
			cleaned = append(cleaned, p)

			return nil
		},
	}
	service.RegisterServer(service.Name("fake-cleanup"), hooked)
	service.RegisterServer(service.Name("fake-cleanup-nil"), service.ServerDriver[fakeConfig]{Bit: 1 << 14})
	t.Cleanup(func() {
		service.DeregisterForTest(service.Name("fake-cleanup"))
		service.DeregisterForTest(service.Name("fake-cleanup-nil"))
	})

	p := serverPlacementFixture(t)
	require.NoError(t, service.CleanupServer(t.Context(), service.Name("fake-cleanup"), p))
	require.Equal(t, []service.Placement{p}, cleaned)
	require.NoError(t, service.CleanupServer(t.Context(), service.Name("fake-cleanup-nil"), p))
	require.ErrorIs(t, service.CleanupServer(t.Context(), service.Name("fake-missing"), p), service.ErrUnknownName)
}

func TestName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		assert func(t *testing.T)
		name   string
	}{
		{
			assert: func(t *testing.T) {
				t.Helper()

				_, err := service.ParseName("pptp")
				require.ErrorIs(t, err, service.ErrUnknownName)
				require.EqualError(t, err, "unknown service name: \"pptp\"")
			},
			name: "parse unregistered",
		},
		{
			assert: func(t *testing.T) {
				t.Helper()

				service.RegisterClient(service.Name("fake-zed"), fakeClientDriver(1<<23))
				service.RegisterClient(service.Name("fake-alpha"), fakeClientDriver(1<<22))
				t.Cleanup(func() {
					service.DeregisterForTest(service.Name("fake-zed"))
					service.DeregisterForTest(service.Name("fake-alpha"))
				})

				names := service.Names()
				require.Less(t, slices.Index(names, service.Name("fake-alpha")), slices.Index(names, service.Name("fake-zed")))
				require.True(t, slices.IsSorted(names))
			},
			name: "names sorted",
		},
		{
			assert: func(t *testing.T) {
				t.Helper()

				require.Zero(t, service.Name("fake-none").Bit())
			},
			name: "bit unregistered",
		},
		{
			assert: func(t *testing.T) {
				t.Helper()

				raw, err := json.Marshal(service.Name("fake-unregistered"))
				require.NoError(t, err)
				require.Equal(t, `"fake-unregistered"`, string(raw))

				_, err = json.Marshal(service.Name("Bad"))
				require.ErrorIs(t, err, service.ErrInvalidName)
			},
			name: "marshal",
		},
		{
			assert: func(t *testing.T) {
				t.Helper()

				var name service.Name

				err := json.Unmarshal([]byte(`"fake-nope"`), &name)
				require.ErrorIs(t, err, service.ErrUnknownName)
			},
			name: "unmarshal unregistered",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.assert(t)
		})
	}
}

func TestPeerRequest_Unpack(t *testing.T) {
	t.Parallel()

	name := service.Name("fake-peer")
	service.RegisterClient(name, fakeClientDriver(1<<19))
	t.Cleanup(func() { service.DeregisterForTest(name) })

	type payload struct {
		PublicKey string `json:"public_key"`
	}

	input := payload{PublicKey: "abc"}
	req, err := service.PackRequest(name, input)
	require.NoError(t, err)

	raw, err := json.Marshal(req)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"fake-peer","payload":{"public_key":"abc"}}`, string(raw))

	var back service.PeerRequest
	require.NoError(t, json.Unmarshal(raw, &back))

	var decoded payload
	require.NoError(t, back.Unpack(&decoded))
	require.Equal(t, "abc", decoded.PublicKey)

	mdPayload := payload{PublicKey: "srv"}
	metadata, err := service.PackMetadata(name, mdPayload)
	require.NoError(t, err)
	require.Equal(t, name, metadata.Name)
	require.NoError(t, metadata.Unpack(&decoded))
	require.Equal(t, "srv", decoded.PublicKey)
}

func TestPlacement_ToolPath(t *testing.T) {
	t.Parallel()

	p := service.Placement{Interface: "dvpn-c0", ToolDir: "/etc/fake-tool/sentinel-dvpn"}

	require.Equal(t, filepath.Join(p.ToolDir, "dvpn-c0.tls.crt"), p.ToolPath(".tls.crt"))
}

func TestPreflightClient(t *testing.T) {
	t.Parallel()

	hooked := service.ClientDriver[fakeConfig]{
		Bit:      1 << 24,
		Defaults: func() fakeConfig { return fakeConfig{} },
		Metadata: func() service.Validator { return &fakePayload{} },
		New:      func(fakeConfig, service.Placement, zerolog.Logger) (service.Client, error) { return nil, nil },
		Preflight: func(_ context.Context, cfg fakeConfig) []service.PreflightCheck {
			return []service.PreflightCheck{{Detail: strconv.Itoa(int(cfg.Port)), Name: "fake"}}
		},
	}
	service.RegisterClient(service.Name("fake-preflight"), hooked)
	service.RegisterClient(service.Name("fake-preflight-nil"), fakeClientDriver(1<<18))
	t.Cleanup(func() {
		service.DeregisterForTest(service.Name("fake-preflight"))
		service.DeregisterForTest(service.Name("fake-preflight-nil"))
	})

	cases := []struct {
		cfg     any
		err     error
		name    string
		service service.Name
		want    []service.PreflightCheck
	}{
		{
			cfg:     fakeConfig{},
			name:    "nil hook",
			service: service.Name("fake-preflight-nil"),
		},
		{
			cfg:     fakeConfig{Port: 5},
			name:    "typed config",
			service: service.Name("fake-preflight"),
			want:    []service.PreflightCheck{{Detail: "5", Name: "fake"}},
		},
		{
			cfg:     &fakeConfig{Port: 5},
			name:    "pointer config",
			service: service.Name("fake-preflight"),
			want:    []service.PreflightCheck{{Detail: "5", Name: "fake"}},
		},
		{
			cfg:     "nope",
			err:     service.ErrUnexpectedConfigType,
			name:    "wrong type",
			service: service.Name("fake-preflight"),
		},
		{
			cfg:     fakeConfig{},
			err:     service.ErrUnknownName,
			name:    "unregistered",
			service: service.Name("fake-nohook"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := service.PreflightClient(t.Context(), tc.service, tc.cfg)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRegisterClient_Panics(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		register func()
		want     string
	}{
		{
			name: "invalid name",
			register: func() {
				service.RegisterClient(service.Name("Bad Name"), fakeClientDriver(1<<20))
			},
			want: "registering \"Bad Name\": name must match ^[a-z][a-z0-9]*(-[a-z0-9]+)*$",
		},
		{
			name: "bit shape",
			register: func() {
				service.RegisterClient(service.Name("fake-bits"), fakeClientDriver(3))
			},
			want: "registering \"fake-bits\": bit 0x3 must have exactly one bit set",
		},
		{
			name: "bit mismatch",
			register: func() {
				service.RegisterClient(service.Name("fake-mismatch"), fakeClientDriver(1<<28))
				service.RegisterServer(service.Name("fake-mismatch"), service.ServerDriver[fakeConfig]{Bit: 1 << 27})
			},
			want: "registering \"fake-mismatch\": bit 0x8000000 conflicts with the registered bit 0x10000000",
		},
		{
			name: "bit taken",
			register: func() {
				service.RegisterClient(service.Name("fake-owner"), fakeClientDriver(1<<26))
				service.RegisterClient(service.Name("fake-thief"), fakeClientDriver(1<<26))
			},
			want: "registering \"fake-thief\": bit 0x4000000 is bound to \"fake-owner\"",
		},
		{
			name: "nil metadata",
			register: func() {
				driver := fakeClientDriver(1 << 17)
				driver.Metadata = nil
				service.RegisterClient(service.Name("fake-nomd"), driver)
			},
			want: "registering \"fake-nomd\": client metadata hook is required",
		},
		{
			name: "duplicate kind",
			register: func() {
				service.RegisterClient(service.Name("fake-twice"), fakeClientDriver(1<<21))
				service.RegisterClient(service.Name("fake-twice"), fakeClientDriver(1<<21))
			},
			want: "registering \"fake-twice\": client already registered",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// A case may register one name before its expected panic; undo it so -count>1 is clean.
			t.Cleanup(func() {
				for _, n := range []string{"fake-mismatch", "fake-owner", "fake-twice"} {
					service.DeregisterForTest(service.Name(n))
				}
			})

			require.PanicsWithError(t, tc.want, tc.register)
		})
	}
}

func TestRegisterServer(t *testing.T) {
	t.Parallel()

	defaults := func() fakeConfig { return fakeConfig{Port: 7} }
	construct := func(cfg fakeConfig, _ service.Placement, _ zerolog.Logger) (service.Server, error) {
		return &fakeServer{cfg: cfg}, nil
	}
	driver := service.ServerDriver[fakeConfig]{
		Binaries: []string{"fake", "fake-quick"},
		Bit:      fakeBit,
		Defaults: defaults,
		New:      construct,
	}
	service.RegisterServer(fakeName, driver)

	clientDefaults := func() fakeConfig { return fakeConfig{Port: 8} }
	constructClient := func(cfg fakeConfig, _ service.Placement, _ zerolog.Logger) (service.Client, error) {
		return nil, nil
	}
	clientDriver := service.ClientDriver[fakeConfig]{
		Binaries: []string{"fake"},
		Bit:      fakeBit,
		Defaults: clientDefaults,
		Metadata: func() service.Validator { return &fakePayload{} },
		New:      constructClient,
	}
	service.RegisterClient(fakeName, clientDriver)
	t.Cleanup(func() { service.DeregisterForTest(fakeName) })

	wantBinaries := []string{"fake", "fake-quick"}
	require.Equal(t, wantBinaries, service.Binaries(fakeName))

	def, err := service.DefaultServerConfig(fakeName)
	require.NoError(t, err)

	wantServerDef := fakeConfig{Port: 7}
	require.Equal(t, wantServerDef, def)

	def, err = service.DefaultClientConfig(fakeName)
	require.NoError(t, err)

	wantClientDef := fakeConfig{Port: 8}
	require.Equal(t, wantClientDef, def)

	target, err := service.EmptyServerConfig(fakeName)
	require.NoError(t, err)

	ptr, ok := target.(*fakeConfig)
	require.True(t, ok)

	ptr.Port = 9

	server, err := service.NewServer(fakeName, ptr, serverPlacementFixture(t), zerolog.Nop())
	require.NoError(t, err)
	require.Equal(t, uint16(9), server.(*fakeServer).cfg.Port)

	cfg10 := fakeConfig{Port: 10}
	server, err = service.NewServer(fakeName, cfg10, serverPlacementFixture(t), zerolog.Nop())
	require.NoError(t, err)
	require.Equal(t, uint16(10), server.(*fakeServer).cfg.Port)

	_, err = service.NewServer(fakeName, "nope", serverPlacementFixture(t), zerolog.Nop())
	require.ErrorIs(t, err, service.ErrUnexpectedConfigType)

	_, err = service.NewServer(service.Name("fake-unregistered"), fakeConfig{}, serverPlacementFixture(t), zerolog.Nop())
	require.ErrorIs(t, err, service.ErrUnknownName)

	unnamed := serverPlacementFixture(t)
	unnamed.Interface = ""
	_, err = service.NewServer(fakeName, cfg10, unnamed, zerolog.Nop())
	require.ErrorIs(t, err, validate.ErrInvalidConfig)

	relative := serverPlacementFixture(t)
	relative.ToolDir = "sentinel-dvpn"
	_, err = service.NewServer(fakeName, cfg10, relative, zerolog.Nop())
	require.ErrorIs(t, err, validate.ErrInvalidConfig)

	_, err = service.DefaultServerConfig(service.Name("fake-unregistered"))
	require.ErrorIs(t, err, service.ErrUnknownName)
}

func TestStopOnce_Do(t *testing.T) {
	t.Parallel()

	var (
		calls int
		once  service.StopOnce
	)

	errStop := errors.New("stop failed")

	require.ErrorIs(t, once.Do(func() error {
		calls++

		return errStop
	}), errStop)
	require.ErrorIs(t, once.Do(func() error {
		calls++

		return nil
	}), errStop)
	require.Equal(t, 1, calls)
}

func TestToolDirs(t *testing.T) {
	t.Parallel()

	name := service.Name("fake-tool")
	clientDriver := fakeClientDriver(1 << 29)
	clientDriver.ToolDir = "/etc/fake-tool/sentinel-dvpn"
	service.RegisterClient(name, clientDriver)
	service.RegisterServer(name, service.ServerDriver[fakeConfig]{
		Bit:     1 << 29,
		ToolDir: "/etc/fake-tool/server/sentinel-dvpn",
	})
	t.Cleanup(func() { service.DeregisterForTest(name) })

	dir, err := service.ClientToolDir(name)
	require.NoError(t, err)
	require.Equal(t, "/etc/fake-tool/sentinel-dvpn", dir)

	dir, err = service.ServerToolDir(name)
	require.NoError(t, err)
	require.Equal(t, "/etc/fake-tool/server/sentinel-dvpn", dir)

	_, err = service.ClientToolDir(service.Name("fake-missing"))
	require.ErrorIs(t, err, service.ErrUnknownName)

	_, err = service.ServerToolDir(service.Name("fake-missing"))
	require.ErrorIs(t, err, service.ErrUnknownName)

	dirs := service.ToolDirs()
	require.Subset(t, dirs, []string{"/etc/fake-tool/server/sentinel-dvpn", "/etc/fake-tool/sentinel-dvpn"})
	require.NotContains(t, dirs, "")
	require.True(t, slices.IsSorted(dirs))
}

func TestUnpackMetadata(t *testing.T) {
	t.Parallel()

	requireUnpackCases(
		t, "decoding service metadata:", validate.ErrInvalidConfig,
		func(raw json.RawMessage) (fakePayload, error) {
			return service.UnpackMetadata[fakePayload](service.ServiceMetadata{Payload: raw})
		},
		func(raw json.RawMessage) (*fakePayload, error) {
			return service.UnpackMetadata[*fakePayload](service.ServiceMetadata{Payload: raw})
		},
	)
}

func TestUnpackRequest(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		payload    string
		want       fakePayload
		wantErr    error
		wantPrefix string
	}{
		{
			name:    "valid",
			payload: `{"name":"a"}`,
			want:    fakePayload{Name: "a"},
		},
		{
			name:       "malformed",
			payload:    `{`,
			wantPrefix: "decoding peer request:",
		},
		{
			name:    "invalid",
			payload: `{"name":""}`,
			wantErr: errFakePayload,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := service.PeerRequest{
				Name:    service.Name("fake-peer"),
				Payload: json.RawMessage(tc.payload),
			}

			got, err := service.UnpackRequest[fakePayload](req)
			require.Equal(t, tc.want, got)

			switch {
			case tc.wantPrefix != "":
				require.Error(t, err)
				require.True(t, strings.HasPrefix(err.Error(), tc.wantPrefix))
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
			default:
				require.NoError(t, err)
			}
		})
	}
}

func TestUnpackResponse(t *testing.T) {
	t.Parallel()

	requireUnpackCases(
		t, "decoding peer response:", service.ErrInvalidPeer,
		func(raw json.RawMessage) (fakePayload, error) {
			return service.UnpackResponse[fakePayload](service.PeerResponse{Payload: raw})
		},
		func(raw json.RawMessage) (*fakePayload, error) {
			return service.UnpackResponse[*fakePayload](service.PeerResponse{Payload: raw})
		},
	)
}

func TestValidateHostname(t *testing.T) {
	t.Parallel()

	label63 := strings.Repeat("a", 63)
	name253 := strings.Join([]string{label63, label63, label63, strings.Repeat("a", 61)}, ".")

	cases := []struct {
		host    string
		name    string
		wantErr bool
	}{
		{host: "node.example", name: "simple"},
		{host: "a.b.c", name: "multi label"},
		{host: "xn--bcher-kva.example", name: "punycode"},
		{host: name253, name: "max length"},
		{host: "-a.example", name: "leading hyphen", wantErr: true},
		{host: "a-.example", name: "trailing hyphen", wantErr: true},
		{host: "a..b", name: "empty label", wantErr: true},
		{host: "node.example.", name: "trailing dot", wantErr: true},
		{host: label63 + "a.example", name: "long label", wantErr: true},
		{host: name253 + "a", name: "long name", wantErr: true},
		{host: "node_1.example", name: "underscore", wantErr: true},
		{host: "", name: "empty", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := service.ValidateHostname(tc.host)
			if tc.wantErr {
				require.ErrorIs(t, err, service.ErrInvalidHostname)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateMetadata(t *testing.T) {
	t.Parallel()

	name := service.Name("fake-metadata")
	service.RegisterClient(name, fakeClientDriver(1<<16))
	t.Cleanup(func() { service.DeregisterForTest(name) })

	cases := []struct {
		md      service.ServiceMetadata
		name    string
		wantErr error
	}{
		{
			md:   service.ServiceMetadata{Name: name, Payload: json.RawMessage(`{"name":"a"}`)},
			name: "valid",
		},
		{
			md:      service.ServiceMetadata{Name: name, Payload: json.RawMessage(`{"name":""}`)},
			name:    "invalid payload",
			wantErr: errFakePayload,
		},
		{
			md:      service.ServiceMetadata{Name: "fake-unregistered", Payload: json.RawMessage(`{}`)},
			name:    "unknown name",
			wantErr: service.ErrUnknownName,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := service.ValidateMetadata(tc.md)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateUUID(t *testing.T) {
	t.Parallel()

	require.NoError(t, service.ValidateUUID("00000000-0000-0000-0000-000000000001"))
	require.ErrorIs(t, service.ValidateUUID("nope"), service.ErrInvalidPeer)
}

func serverPlacementFixture(t *testing.T) service.Placement {
	t.Helper()

	return service.Placement{
		Dir:       t.TempDir(),
		Interface: "dvpn-n0",
		Table:     "sentinel-dvpn-n0",
		ToolDir:   t.TempDir(),
	}
}
