package client_test

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	daemonv1 "github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
	"github.com/sentinel-official/sentinel-go-sdk/v3/nodeapi"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/session"
)

func TestIssueErrorReason(t *testing.T) {
	t.Parallel()

	cases := []struct {
		err  error
		name string
	}{
		{
			err:  api.ErrInvalidResponse,
			name: "invalid response",
		},
		{
			err:  api.ErrResponseTooLarge,
			name: "response too large",
		},
		{
			err:  api.ErrUnexpectedRedirect,
			name: "unexpected redirect",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, "invalid node response", client.IssueErrorReason(fmt.Errorf("getting session: %w", tc.err)))
		})
	}
}

func TestRuntime_IssueLoop_AbortsOnOvercharge(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 100_000, false)
	row := startIssuer(t, rt, home)

	state.SetLocalStats(service.PeerStats{RxBytes: 1_000_000_000})
	home.NodeService.SetUsage(session.Usage{RxBytes: 1_060_000_000})
	home.NodeService.PublishVoucherRequest()
	waitIssuerEnded(t, home, row.ID, "node overcharging")
	require.Zero(t, home.NodeService.VoucherCount())
}

func TestRuntime_IssueLoop_EndsAfterRepeatedStatsFailures(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 100_000, false)
	client.SetTiming(rt, 20*time.Millisecond, time.Hour, 8, 30*time.Millisecond)

	var failing atomic.Bool

	statsError := func() error {
		if failing.Load() {
			return errors.New("peer counters unavailable")
		}

		return nil
	}
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{
		hooks: clientHooks{onStatsError: &statsError},
		state: state,
	})
	row := startIssuer(t, rt, home)

	failing.Store(true)

	waitIssuerEnded(t, home, row.ID, "service stats unavailable")
}

func TestRuntime_IssueLoop_EndsOnClosure(t *testing.T) {
	t.Parallel()

	cases := []struct {
		escrowExhausted bool
		name            string
		poll            bool
		reason          string
		status          nodeapi.SessionStatus
		want            string
	}{
		{escrowExhausted: true, name: "stream credit exhausted", reason: "credit exhausted", want: "escrow exhausted"},
		{name: "stream operator", reason: "closed by operator", want: "closed by operator"},
		{
			escrowExhausted: true,
			name:            "poll closed credit exhausted",
			poll:            true,
			reason:          "credit exhausted",
			status:          nodeapi.SessionStatusClosed,
			want:            "escrow exhausted",
		},
		{
			name:   "poll closing lease not funded",
			poll:   true,
			reason: "lease not funded",
			status: nodeapi.SessionStatusClosing,
			want:   "lease not funded",
		},
		{
			name:   "poll closing missing reason",
			poll:   true,
			status: nodeapi.SessionStatusClosing,
			want:   "invalid node response",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rt, home, state := newIssuerFixture(t, 1_000, tc.poll)
			row := startIssuer(t, rt, home)

			if tc.escrowExhausted {
				state.SetLocalStats(service.PeerStats{RxBytes: 150_000_000})
				home.NodeService.SetUsage(session.Usage{RxBytes: 150_000_000})
				require.NoError(t, client.SampleAndIssue(t.Context(), rt))
				require.Equal(t, uint64(1000), home.NodeService.Voucher(0).Cumulative)
			}

			if tc.poll {
				setNodeResponse(t, rt, http.StatusOK, nodeapi.Session{
					CloseReason: nodeapi.ClosedReason(tc.reason),
					Status:      tc.status,
				})
			} else {
				home.NodeService.PublishClosed(tc.reason)
			}

			waitIssuerEnded(t, home, row.ID, tc.want)
		})
	}
}

func TestRuntime_IssueLoop_EndsTicketOnClosedEvent(t *testing.T) {
	t.Parallel()

	fixture := newTicketFixture(t)
	req := seedTicketRequest(
		t, fixture.rpc, fixture.home, fixture.owner, time.Now().Add(time.Hour).Unix(),
	)
	row, err := client.StartSession(t.Context(), fixture.rt, req)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return fixture.home.NodeService.StreamOpenCount() == 1
	}, time.Second, time.Millisecond)
	fixture.home.NodeService.PublishClosed("credit exhausted")
	waitIssuerEnded(t, fixture.home, row.ID, "credit exhausted")
	require.Zero(t, fixture.home.NodeService.VoucherCount())
}

func TestRuntime_IssueLoop_IgnoresForgedCumulative(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 1_000, false)
	client.SetTiming(rt, time.Hour, time.Hour, 8, 30*time.Millisecond)

	row := startIssuer(t, rt, home)
	setNodeResponse(t, rt, http.StatusOK, nodeapi.Session{
		Ledger: nodeapi.Ledger{Basis: session.BasisPerGB, CumulativeBaseUnits: 1_000},
		Status: nodeapi.SessionStatusActive,
	})
	require.NoError(t, client.Poll(t.Context(), rt))
	state.SetLocalStats(service.PeerStats{RxBytes: 50_000_000})
	setNodeResponse(t, rt, http.StatusOK, nodeapi.SendVoucherResponse{
		Consumption: nodeapi.Consumption{
			OwedBaseUnits: 250,
			Usage:         session.Usage{RxBytes: 50_000_000},
		},
		Ledger: nodeapi.Ledger{Basis: session.BasisPerGB, CumulativeBaseUnits: 1_000},
	})
	require.NoError(t, client.SampleAndIssue(t.Context(), rt))

	var vouchers []client.Voucher
	require.NoError(t, home.DB.WithContext(t.Context()).Where("session_id = ?", row.ID).Find(&vouchers).Error)
	require.Len(t, vouchers, 1)
	require.Equal(t, uint64(750), vouchers[0].Cumulative)

	var stored client.Session
	require.NoError(t, home.DB.WithContext(t.Context()).First(&stored, "id = ?", row.ID).Error)
	require.Equal(t, uint64(750), stored.PaidBaseUnits)

	var channel client.Channel
	require.NoError(t, home.DB.WithContext(t.Context()).First(&channel, "address = ?", row.ChannelAddress).Error)
	require.Equal(t, uint64(750), channel.Cumulative)
	home.NodeService.PublishClosed("credit exhausted")
	waitIssuerEnded(t, home, row.ID, "credit exhausted")
}

func TestRuntime_IssueLoop_PaysOnVoucherRequest(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 100_000, false)
	startIssuer(t, rt, home)
	state.SetLocalStats(service.PeerStats{RxBytes: 48_000_000})
	home.NodeService.SetUsage(session.Usage{RxBytes: 50_000_000})
	home.NodeService.PublishVoucherRequest()
	require.Eventually(t, func() bool { return home.NodeService.VoucherCount() == 1 }, time.Second, time.Millisecond)
	require.Equal(t, uint64(750), home.NodeService.Voucher(0).Cumulative)

	_, active := client.Current(rt)
	require.True(t, active)
}

func TestRuntime_IssueLoop_PersistsAcceptedVoucherOnStatsFailure(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 100_000, false)
	client.SetTiming(rt, time.Hour, time.Hour, 8, 30*time.Millisecond)

	statsErr := errors.New("peer counters unavailable")
	calls := 0
	statsError := func() error {
		calls++
		if calls == 2 {
			return statsErr
		}

		return nil
	}
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{
		hooks: clientHooks{onStatsError: &statsError},
		state: state,
	})
	row := startIssuer(t, rt, home)

	state.SetLocalStats(service.PeerStats{RxBytes: 50_000_000})
	home.NodeService.SetUsage(session.Usage{RxBytes: 50_000_000})
	require.ErrorIs(t, client.SampleAndIssue(t.Context(), rt), statsErr)

	var stored client.Session
	require.NoError(t, home.DB.WithContext(t.Context()).First(&stored, "id = ?", row.ID).Error)
	require.Equal(t, uint64(750), stored.PaidBaseUnits)

	var channel client.Channel
	require.NoError(t, home.DB.WithContext(t.Context()).First(&channel, "address = ?", row.ChannelAddress).Error)
	require.Equal(t, uint64(750), channel.Cumulative)

	var vouchers []client.Voucher
	require.NoError(t, home.DB.WithContext(t.Context()).Where("session_id = ?", row.ID).Find(&vouchers).Error)
	require.Len(t, vouchers, 1)
	require.Equal(t, uint64(750), vouchers[0].Cumulative)
	require.Equal(t, home.NodeService.Voucher(0).Signature, vouchers[0].Signature)
	require.NoError(t, client.SampleAndIssue(t.Context(), rt))
	require.Equal(t, 1, home.NodeService.VoucherCount())
}

func TestRuntime_IssueLoop_PersistsSampleOnVoucherFailure(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 100_000, false)
	row := startIssuer(t, rt, home)
	setNodeResponse(t, rt, http.StatusTooManyRequests, api.ErrorResponse{Error: *api.ErrRateLimited})
	state.SetLocalStats(service.PeerStats{RxBytes: 50_000_000})
	require.ErrorIs(t, client.SampleAndIssue(t.Context(), rt), api.ErrRateLimited)

	var stored client.Session
	require.NoError(t, home.DB.WithContext(t.Context()).First(&stored, "id = ?", row.ID).Error)
	require.Equal(t, uint64(50_000_000), stored.RxBytes)

	_, active := client.Current(rt)
	require.True(t, active)

	saveErr := errors.New("session store unavailable")
	callback := "test:fail_session_save"
	require.NoError(t, home.DB.Callback().Update().Before("gorm:update").Register(callback, func(db *gorm.DB) {
		if db.Statement.Table == "sessions" {
			_ = db.AddError(saveErr)
		}
	}))
	t.Cleanup(func() { require.NoError(t, home.DB.Callback().Update().Remove(callback)) })
	state.SetLocalStats(service.PeerStats{RxBytes: 100_000_000})

	err := client.SampleAndIssue(t.Context(), rt)
	require.ErrorIs(t, err, api.ErrRateLimited)
	require.ErrorIs(t, err, saveErr)
}

func TestRuntime_IssueLoop_PollsWhenStreamSilent(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 100_000, true)
	startIssuer(t, rt, home)
	require.Eventually(t, func() bool {
		return home.NodeService.GetSessionCalls() >= 2
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return home.NodeService.StreamOpenCount() >= 2
	}, 5*time.Second, 10*time.Millisecond)

	_, active := client.Current(rt)
	require.True(t, active)

	state.SetLocalStats(service.PeerStats{RxBytes: 50_000_000})
	home.NodeService.SetUsage(session.Usage{RxBytes: 50_000_000})
	require.NoError(t, client.SampleAndIssue(t.Context(), rt))
	require.Equal(t, 1, home.NodeService.VoucherCount())
	calls := home.NodeService.GetSessionCalls()

	require.Eventually(t, func() bool { return home.NodeService.GetSessionCalls() > calls }, time.Second, time.Millisecond)
}

func TestRuntime_IssueLoop_PollsWhileStreamHealthy(t *testing.T) {
	t.Parallel()

	rt, home, _ := newIssuerFixture(t, 100_000, false)
	startIssuer(t, rt, home)

	require.Eventually(t, func() bool {
		return home.NodeService.GetSessionCalls() >= 2
	}, 3*time.Second, 10*time.Millisecond)
}

func TestRuntime_IssueLoop_ReconnectsOnNodeTimeout(t *testing.T) {
	t.Parallel()

	rt, home, _ := newIssuerFixture(t, 100_000, true)
	home.Loaded.Client.NodeTimeoutSeconds = 2
	home.NodeService.SetGetSessionErr(api.ErrInternal, 1000)
	startIssuer(t, rt, home)
	require.Eventually(t, func() bool { return home.NodeService.OpenedCount() >= 2 }, 5*time.Second, 10*time.Millisecond)
}

func TestRuntime_IssueLoop_ReportsReconnectingWhileNodeUnreachable(t *testing.T) {
	t.Parallel()

	// A silent stream: polls are the only contact. The node timeout is far away, so only the status can tell.
	rt, home, _ := newIssuerFixture(t, 100_000, true)
	home.Loaded.Client.NodeTimeoutSeconds = 3600
	row := startIssuer(t, rt, home)

	home.NodeService.SetGetSessionErr(api.ErrInternal, 1_000_000)
	require.Eventually(t, func() bool {
		current, ok := client.Current(rt)

		return ok && current.ID == row.ID && current.Status == client.SessionStatusReconnecting
	}, 5*time.Second, 10*time.Millisecond)

	home.NodeService.SetGetSessionErr(nil, 0)
	require.Eventually(t, func() bool {
		current, ok := client.Current(rt)

		return ok && current.ID == row.ID && current.Status == client.SessionStatusActive
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, home.NodeService.OpenedCount())
}

// redialWindow times the node out and returns while the daemon waits to redial (backoff 2 s before attempt 1).
func redialWindow(t *testing.T) (*client.Runtime, testutil.Home, *client.Session) {
	t.Helper()

	rt, home, _ := newIssuerFixture(t, 100_000, true)
	client.SetTiming(rt, time.Hour, 50*time.Millisecond, 3, 2*time.Second)
	home.Loaded.Client.NodeTimeoutSeconds = 1
	row := startIssuer(t, rt, home)

	home.NodeService.SetGetSessionErr(api.ErrInternal, 1_000_000)
	waitIssuerEnded(t, home, row.ID, "connection lost")

	return rt, home, row
}

func TestRuntime_Reconnect_ShowsTheRedialledSession(t *testing.T) {
	t.Parallel()

	rt, _, row := redialWindow(t)
	current, ok := client.Current(rt)
	require.True(t, ok, "status must not say 'no session' while the daemon redials")
	require.Equal(t, row.ID, current.ID)
	require.Equal(t, client.SessionStatusReconnecting, current.Status)
}

func TestRuntime_Reconnect_StopByIDCancelsTheRedial(t *testing.T) {
	t.Parallel()

	rt, home, row := redialWindow(t)
	require.NoError(t, client.StopSessionByID(t.Context(), rt, row.ID))

	_, ok := client.Current(rt)
	require.False(t, ok)

	home.NodeService.SetGetSessionErr(nil, 0)
	time.Sleep(3 * time.Second) // past the first redial attempt
	require.Equal(t, 1, home.NodeService.OpenedCount(), "a stopped redial must not open a session")
}

func TestRuntime_Reconnect_StartReplacesTheRedial(t *testing.T) {
	t.Parallel()

	rt, home, row := redialWindow(t)
	home.NodeService.SetGetSessionErr(nil, 0)

	req := &daemonv1.StartSessionRequest{
		Credential: &daemonv1.Credential{Kind: &daemonv1.Credential_Channel{Channel: row.ChannelAddress}},
		Node:       home.NodeAddress.String(),
	}
	started, err := client.StartSession(t.Context(), rt, req)
	require.NoError(t, err, "a start while the daemon redials must not fail with 'a session is already active'")
	require.NotEqual(t, row.ID, started.ID)

	current, ok := client.Current(rt)
	require.True(t, ok)
	require.Equal(t, started.ID, current.ID)
	require.Equal(t, client.SessionStatusActive, current.Status)

	time.Sleep(3 * time.Second) // past the cancelled redial's first attempt
	require.Equal(t, 2, home.NodeService.OpenedCount(), "the replaced redial must not open a session")
}

func TestRuntime_IssueLoop_ReconnectsWhenVoucherPostsFail(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 100_000, false)
	home.Loaded.Client.NodeTimeoutSeconds = 2
	home.NodeService.SetSendChannelVoucherErr(api.ErrInternal, 1000)
	startIssuer(t, rt, home)

	state.SetLocalStats(service.PeerStats{RxBytes: 50_000_000})
	home.NodeService.SetUsage(session.Usage{RxBytes: 50_000_000})
	require.Eventually(t, func() bool {
		return home.NodeService.GetSessionCalls() >= 2
	}, 3*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return home.NodeService.OpenedCount() >= 2
	}, 5*time.Second, 10*time.Millisecond)
}

func TestRuntime_IssueLoop_RecoversTopUpExposureAfterCrash(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 100_000, false)
	row := startIssuer(t, rt, home)

	state.SetLocalStats(service.PeerStats{RxBytes: 50_000_000})
	home.NodeService.SetUsage(session.Usage{RxBytes: 50_000_000})
	require.NoError(t, client.SampleAndIssue(t.Context(), rt))
	client.ClearConnection(rt)

	restarted := newRuntime(t, home, home.ProgramClient)
	client.SetFirewallBackend(restarted, &recordingFirewallBackend{})
	client.SetDNSBackend(restarted, &recordingDNSBackend{})
	client.SetService(restarted, sdktest.FakeName, fakeClientConfig{state: state})
	require.NoError(t, restarted.Recover(t.Context()))
	bound, err := client.SettledBound(t.Context(), restarted, row.ChannelAddress, 500, 5)
	require.NoError(t, err)
	require.Equal(t, uint64(750), bound)
	home.NodeService.SetUsage(session.Usage{})
	home.NodeService.SetSettled(600)
	state.SetLocalStats(service.PeerStats{})

	req := &daemonv1.StartSessionRequest{
		Credential: &daemonv1.Credential{Kind: &daemonv1.Credential_Channel{Channel: row.ChannelAddress}},
		Node:       home.NodeAddress.String(),
	}
	resumed, err := client.StartSession(t.Context(), restarted, req)
	require.NoError(t, err)
	require.Equal(t, uint64(600), resumed.SettledBaseUnits)
}

func TestRuntime_IssueLoop_ResamplesBeforeVoucherAudit(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 100_000, false)

	var advance sync.Once

	onStats := func() {
		advance.Do(func() {
			state.SetLocalStats(service.PeerStats{RxBytes: 160_000_000})
			home.NodeService.SetUsage(session.Usage{RxBytes: 160_000_000})
		})
	}
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{
		hooks: clientHooks{onStats: &onStats},
		state: state,
	})
	startIssuer(t, rt, home)
	state.SetLocalStats(service.PeerStats{RxBytes: 50_000_000})
	require.NoError(t, client.SampleAndIssue(t.Context(), rt))
	snapshot, active := client.Current(rt)
	require.True(t, active)
	require.Equal(t, uint64(160_000_000), snapshot.RxBytes)
}

func TestRuntime_IssueLoop_ResetsStatsFailuresAfterSuccess(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 100_000, false)
	client.SetTiming(rt, time.Hour, time.Hour, 8, 30*time.Millisecond)

	var failing atomic.Bool

	statsErr := errors.New("peer counters unavailable")
	statsError := func() error {
		if failing.Load() {
			return statsErr
		}

		return nil
	}
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{
		hooks: clientHooks{onStatsError: &statsError},
		state: state,
	})
	row := startIssuer(t, rt, home)

	failing.Store(true)

	for range 4 {
		require.ErrorIs(t, client.SampleAndIssue(t.Context(), rt), statsErr)
	}

	failing.Store(false)
	require.NoError(t, client.SampleAndIssue(t.Context(), rt))
	failing.Store(true)

	for range 4 {
		require.ErrorIs(t, client.SampleAndIssue(t.Context(), rt), statsErr)
	}

	current, active := client.Current(rt)
	require.True(t, active)
	require.Equal(t, row.ID, current.ID)
	home.NodeService.PublishVoucherRequest()
	waitIssuerEnded(t, home, row.ID, "service stats unavailable")
}

func TestRuntime_IssueLoop_SerializesConcurrentVouchers(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 100_000, false)
	startIssuer(t, rt, home)
	state.SetLocalStats(service.PeerStats{RxBytes: 50_000_000})
	home.NodeService.SetUsage(session.Usage{RxBytes: 50_000_000})

	results := make(chan error, 2)

	for range 2 {
		go func() { results <- client.SampleAndIssue(t.Context(), rt) }()
	}

	home.NodeService.PublishVoucherRequest()

	for range 2 {
		require.NoError(t, <-results)
	}

	require.NoError(t, client.Poll(t.Context(), rt))
	require.Equal(t, 1, home.NodeService.VoucherCount())
}

func TestRuntime_IssueLoop_TopsUpAtHalfBlock(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 100_000, false)
	row := startIssuer(t, rt, home)

	state.SetLocalStats(service.PeerStats{RxBytes: 49_800_000})
	require.NoError(t, client.SampleAndIssue(t.Context(), rt))
	require.Zero(t, home.NodeService.VoucherCount())

	state.SetLocalStats(service.PeerStats{RxBytes: 50_000_000})
	home.NodeService.SetUsage(session.Usage{RxBytes: 50_000_000})
	require.NoError(t, client.SampleAndIssue(t.Context(), rt))
	require.Equal(t, 1, home.NodeService.VoucherCount())
	require.Equal(t, uint64(750), home.NodeService.Voucher(0).Cumulative)

	var stored client.Session
	require.NoError(t, home.DB.WithContext(t.Context()).First(&stored, "id = ?", row.ID).Error)
	require.Equal(t, uint64(250), stored.SpentBaseUnits)

	var channel client.Channel
	require.NoError(t, home.DB.WithContext(t.Context()).First(&channel, "address = ?", row.ChannelAddress).Error)
	require.Equal(t, uint64(750), channel.Cumulative)
}

func TestRuntime_Poll_PreservesSpentAheadOfNodeUsage(t *testing.T) {
	t.Parallel()

	rt, home, state := newIssuerFixture(t, 100_000, false)
	client.SetTiming(rt, time.Hour, time.Hour, 8, 30*time.Millisecond)
	row := startIssuer(t, rt, home)

	state.SetLocalStats(service.PeerStats{RxBytes: 30_000_000})
	require.NoError(t, client.SampleAndIssue(t.Context(), rt))

	var stored client.Session
	require.NoError(t, home.DB.WithContext(t.Context()).First(&stored, "id = ?", row.ID).Error)
	require.Equal(t, uint64(150), stored.SpentBaseUnits)
	require.Zero(t, home.NodeService.VoucherCount())

	require.NoError(t, client.Poll(t.Context(), rt))
	require.NoError(t, home.DB.WithContext(t.Context()).First(&stored, "id = ?", row.ID).Error)
	require.Equal(t, uint64(150), stored.SpentBaseUnits)
	require.Zero(t, home.NodeService.VoucherCount())
}

func TestRuntime_Supervise_StopsOnInvalidClosedEvent(t *testing.T) {
	t.Parallel()

	rt, home, _ := newIssuerFixture(t, 100_000, false)
	client.SetTiming(rt, time.Hour, time.Hour, 8, 30*time.Millisecond)
	row := startIssuer(t, rt, home)

	home.NodeService.PublishClosed("boom")
	waitIssuerEnded(t, home, row.ID, "invalid node response")
	require.Equal(t, 1, home.NodeService.OpenedCount())
}

func TestRuntime_Supervise_StopsOnInvalidPollResponse(t *testing.T) {
	t.Parallel()

	rt, home, _ := newIssuerFixture(t, 100_000, false)
	row := startIssuer(t, rt, home)

	home.NodeService.SetSessionStatus("weird")
	waitIssuerEnded(t, home, row.ID, "invalid node response")
	require.Equal(t, 1, home.NodeService.OpenedCount())
}

func newIssuerFixture(t *testing.T, escrow uint64, silent bool) (*client.Runtime, testutil.Home, *sdktest.FakeState) {
	t.Helper()

	fakeRPC := sdktest.NewFakeRPC(t)
	home := testutil.NewClientHome(t, fakeRPC)
	home.Loaded.Client.MaxPrepayBytes = 100_000_000
	home.Loaded.Client.EscrowBaseUnits = escrow
	home.NodeService.SetEscrow(escrow)

	if silent {
		home.NodeService.SetHeartbeatInterval(0)
	}

	rt := newRuntime(t, home, installOpenChannelReactor(t, fakeRPC, home))
	client.SetFirewallBackend(rt, &recordingFirewallBackend{})
	client.SetDNSBackend(rt, &recordingDNSBackend{})
	registerFakeClientOnce()

	state := sdktest.NewFakeState(sdktest.FakeName)
	client.SetService(rt, sdktest.FakeName, fakeClientConfig{state: state})
	client.SetTiming(rt, time.Hour, 50*time.Millisecond, 8, 30*time.Millisecond)

	return rt, home, state
}

func startIssuer(t *testing.T, rt *client.Runtime, home testutil.Home) *client.Session {
	t.Helper()

	row, err := client.StartSession(t.Context(), rt, newChannelRequest(home.NodeAddress.String()))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return home.NodeService.StreamOpenCount() >= 1 }, time.Second, time.Millisecond)

	return row
}

func waitIssuerEnded(t *testing.T, home testutil.Home, id, reason string) {
	t.Helper()

	require.Eventually(t, func() bool {
		var row client.Session
		require.NoError(t, home.DB.WithContext(t.Context()).First(&row, "id = ?", id).Error)

		return row.Status == client.SessionStatusEnded && row.Error == reason
	}, 5*time.Second, 5*time.Millisecond)
}

func TestRuntime_Reconnect_ListShowsTheRedialAsReconnecting(t *testing.T) {
	t.Parallel()

	rt, _, row := redialWindow(t)
	listed, err := client.ListSessionsFor(t.Context(), rt)
	require.NoError(t, err)

	var status string
	for _, s := range listed.GetSessions() {
		if s.GetId() == row.ID {
			status = s.GetStatus()
		}
	}
	require.Equal(t, client.SessionStatusReconnecting, status, "the list must agree with status during a redial")
}

// A start that replaces a redial must not set up its session until the old one is torn down: that teardown reverts
// DNS and stops the service, and run alongside the new session it would undo the new session's DNS and firewall.
// What guarantees it is the turn queue: runSession keeps the first start's turn through supervise, and so through
// the redial and its teardown, and the next start's connect waits for that turn.
func TestRuntime_Reconnect_StartWaitsForTheRedialTeardown(t *testing.T) {
	t.Parallel()

	rt, home, _ := newIssuerFixture(t, 100_000, true)
	client.SetTiming(rt, time.Hour, 50*time.Millisecond, 3, 2*time.Second)
	home.Loaded.Client.NodeTimeoutSeconds = 1
	row := startIssuer(t, rt, home)

	entered, release := home.NodeService.HoldClose()
	t.Cleanup(release)
	home.NodeService.SetGetSessionErr(api.ErrInternal, 1_000_000)

	select {
	case <-entered: // the redial published its placeholder and its teardown is closing the old session
	case <-time.After(10 * time.Second):
		t.Fatal("the redial never closed the old session")
	}
	home.NodeService.SetGetSessionErr(nil, 0)

	req := &daemonv1.StartSessionRequest{
		Credential: &daemonv1.Credential{Kind: &daemonv1.Credential_Channel{Channel: row.ChannelAddress}},
		Node:       home.NodeAddress.String(),
	}

	type result struct {
		s   *client.Session
		err error
	}
	done := make(chan result, 1)
	go func() {
		s, err := client.StartSession(t.Context(), rt, req)
		done <- result{s, err}
	}()

	select {
	case got := <-done:
		t.Fatalf("the start returned (%v) while the old session's teardown was still running", got.err)
	case <-time.After(500 * time.Millisecond):
	}

	release()

	var got result
	select {
	case got = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("the start never returned after the teardown finished")
	}
	require.NoError(t, got.err)

	var ended client.Session
	require.NoError(t, home.DB.WithContext(t.Context()).First(&ended, "id = ?", row.ID).Error)
	require.Equal(t, client.SessionStatusEnded, ended.Status, "the old teardown must be done before the new session starts")

	current, ok := client.Current(rt)
	require.True(t, ok)
	require.Equal(t, got.s.ID, current.ID)
}
