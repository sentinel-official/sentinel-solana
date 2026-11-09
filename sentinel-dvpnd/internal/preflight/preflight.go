package preflight

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"slices"
	"strings"

	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2/rpc"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-go-sdk/v3/cli"
	"github.com/sentinel-official/sentinel-go-sdk/v3/paymaster"
	"github.com/sentinel-official/sentinel-go-sdk/v3/program"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

const (
	defaultResolvConfPath    = "/etc/resolv.conf"
	defaultResolvedProbePath = "/run/systemd/resolve/resolv.conf"
	paymasterCheckName       = "paymaster"
)

var ErrPreflightFailed = errors.New("preflight failed")

var (
	errDNSBackendUnusable        = errors.New("client dns backend is not usable")
	errKillSwitchBackendUnusable = errors.New("kill switch backend is not usable")
)

func Run(
	ctx context.Context,
	loaded *config.Loaded,
	log zerolog.Logger,
	rpcClient *rpc.Client,
	extra ...service.PreflightCheck,
) ([]service.PreflightCheck, error) {
	checks := slices.Clone(extra)
	programClient, err := program.NewClientFromBuild(rpcClient, loaded.File.RPC.Commitment)

	checks = append(checks, newCheck("build", err))
	if err != nil {
		return checks, fmt.Errorf("%w: build", ErrPreflightFailed)
	}

	err = loaded.File.RPC.Validate()
	if err == nil {
		rpcCtx, cancel := context.WithTimeout(ctx, loaded.File.RPC.Timeout)
		_, err = rpcClient.GetHealth(rpcCtx)

		cancel()
	}

	checks = append(checks, newCheck("rpc", err))
	if err != nil {
		return checks, fmt.Errorf("%w: rpc", ErrPreflightFailed)
	}

	if loaded.File.Peer.AllowPrivate {
		checks = append(checks, newWarning(
			"peer_allow_private", "private peer addresses are allowed; disable for production",
		))
	}

	if loaded.File.Tx.Paymaster.URL != "" {
		checks = append(checks, checkPaymaster(ctx, loaded, programClient, log))
	}

	checks = append(checks, checkChain(ctx, loaded, programClient)...)
	checks = append(checks, checkRole(ctx, loaded, programClient)...)

	return checks, firstFailure(checks, log)
}

func checkBinaries(name service.Name) error {
	if problem := binariesProblem(name); problem != "" {
		return errors.New(problem)
	}

	return nil
}

func checkDNSAt(backend, resolvConf, resolvedProbe string) error {
	switch backend {
	case config.DNSResolvectl:
		if _, err := service.LookPath("resolvectl"); err != nil {
			return fmt.Errorf("%w: dns_backend = %q: %w; %s", errDNSBackendUnusable, backend, err, dnsBackendHelp("linux"))
		}

		if _, err := os.Stat(resolvedProbe); err != nil {
			return fmt.Errorf(
				"%w: dns_backend = %q: systemd-resolved is not reachable: %w; %s",
				errDNSBackendUnusable, backend, err, dnsBackendHelp("linux"),
			)
		}
	case config.DNSResolvconf:
		file, err := os.OpenFile(resolvConf, os.O_WRONLY, 0) //#nosec G304
		if err != nil {
			return fmt.Errorf(
				"%w: dns_backend = %q: %s is not writable: %w; %s",
				errDNSBackendUnusable, backend, resolvConf, err, dnsBackendHelp("linux"),
			)
		}

		if err := file.Close(); err != nil {
			return fmt.Errorf("%w: dns_backend = %q: closing %s: %w", errDNSBackendUnusable, backend, resolvConf, err)
		}
	}

	return nil
}

func checkDNSBackend(backend string) error {
	switch runtime.GOOS {
	case "darwin":
		return checkDNSTool(backend, config.DNSNetworksetup, "networksetup")
	case "windows":
		return checkDNSTool(backend, config.DNSNetsh, "netsh")
	default:
		return checkDNSAt(backend, defaultResolvConfPath, defaultResolvedProbePath)
	}
}

func checkDNSTool(backend, want, tool string) error {
	if backend != want {
		return nil
	}

	if _, err := service.LookPath(tool); err != nil {
		return fmt.Errorf("%w: dns_backend = %q: %w; %s", errDNSBackendUnusable, backend, err, dnsBackendHelp(runtime.GOOS))
	}

	return nil
}

func checkPaymaster(
	ctx context.Context,
	loaded *config.Loaded,
	programClient *program.Client,
	log zerolog.Logger,
) service.PreflightCheck {
	cfg := loaded.File.Tx.Paymaster
	client := paymaster.NewClient(cfg.URL, cfg.APIKey)

	paymasterCtx, cancel := context.WithTimeout(ctx, loaded.File.RPC.Timeout)
	defer cancel()

	payer, err := client.PayerSigner(paymasterCtx)
	if err != nil {
		return newPaymasterCheck(cfg.Fallback, err)
	}

	price, err := client.Price(paymasterCtx)
	if err != nil {
		return newPaymasterCheck(cfg.Fallback, err)
	}

	feeToken := cfg.FeeToken
	if feeToken == "" {
		feeToken = programClient.Mint.String()
	}

	url := cli.RedactURL(cfg.URL)
	check := service.NewPreflightCheck(
		paymasterCheckName,
		service.SeverityOK,
		fmt.Sprintf("url %s, payer %s, price type %s, fee token %s", url, payer.Signer, price.Type, feeToken),
	)
	log.Info().
		Str("fee_token", feeToken).
		Str("payer", payer.Signer.String()).
		Str("price_type", price.Type).
		Str("url", url).
		Msg("paymaster is reachable")

	return check
}

func checkPortFree(ctx context.Context, port uint16) error {
	var listenConfig net.ListenConfig

	ln, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("port %d is not free: %w", port, err)
	}

	if err := ln.Close(); err != nil {
		return fmt.Errorf("closing probe listener: %w", err)
	}

	return nil
}

func checkRole(ctx context.Context, loaded *config.Loaded, programClient *program.Client) []service.PreflightCheck {
	var checks []service.PreflightCheck

	switch loaded.Role {
	case config.RoleNode:
		name, err := service.ParseName(loaded.Node.Service)
		if err != nil {
			checks = append(checks, newCheck("service_binaries", err))
		} else {
			checks = append(checks, newCheck("service_binaries_"+name.String(), checkBinaries(name)))
			checks = append(checks, hookChecks(ctx, service.PreflightServer, name, loaded.Services[name], "")...)
		}

		checks = append(checks, newCheck("api_port", checkPortFree(ctx, loaded.Node.API.Port)))
	case config.RoleClient:
		checks = append(checks, newCheck("privileges", checkPrivileges()))
		checks = append(checks, newCheck("dns_backend", checkDNSBackend(loaded.Client.Network.DNSBackend)))

		if loaded.Client.Network.KillSwitch != config.KillSwitchOff {
			checks = append(checks, newCheck("kill_switch_backend", checkKillSwitchBackend(ctx)))
		}

		for _, name := range service.Names() {
			if problem := binariesProblem(name); problem != "" {
				checks = append(checks, newWarning("service_binaries_"+name.String(), problem))
			}

			cfg, ok := loaded.Services[name]
			if !ok {
				cfg, _ = service.EmptyClientConfig(name)
			}

			checks = append(checks, hookChecks(ctx, service.PreflightClient, name, cfg, "_"+name.String())...)
		}
	case config.RoleProvider:
		checks = append(checks, newCheck("api_port", checkPortFree(ctx, loaded.Provider.API.Port)))
		checks = append(checks, checkDelegate(ctx, loaded, programClient))
	}

	return checks
}

func dnsBackendHelp(goos string) string {
	switch goos {
	case "darwin":
		return "want networksetup or none (or set SENTINEL_DVPND_NETWORK_DNS_BACKEND)"
	case "windows":
		return "want netsh or none (or set SENTINEL_DVPND_NETWORK_DNS_BACKEND)"
	default:
		return "want resolvectl, resolvconf, or none (or set SENTINEL_DVPND_NETWORK_DNS_BACKEND)"
	}
}

func firstFailure(checks []service.PreflightCheck, log zerolog.Logger) error {
	for _, check := range checks {
		if !check.OK {
			log.Warn().Str("check", check.Name).Str("detail", check.Detail).Msg("preflight check failed")

			return fmt.Errorf("%w: %s", ErrPreflightFailed, check.Name)
		}
	}

	return nil
}

func hookChecks(
	ctx context.Context,
	run func(context.Context, service.Name, any) ([]service.PreflightCheck, error),
	name service.Name,
	cfg any,
	suffix string,
) []service.PreflightCheck {
	checks, err := run(ctx, name, cfg)
	if err != nil {
		return []service.PreflightCheck{newCheck("service_preflight"+suffix, err)}
	}

	for i := range checks {
		checks[i].Name += suffix
	}

	return checks
}

// binariesProblem describes the tools of a service that cannot be run, or returns "". A tool that is
// installed but refused (another account could replace it) is reported as found and refused, with
// how to install it, so an operator is not told it is missing when it is there.
func binariesProblem(name service.Name) string {
	return binariesProblemOf(service.Binaries(name))
}

func binariesProblemOf(binaries []string) string {
	var missing, refused []string

	for _, binary := range binaries {
		_, err := service.LookPath(binary)

		switch {
		case err == nil:
		case errors.Is(err, service.ErrUntrustedBinary):
			refused = append(refused, refusedBinary(binary, err, runtime.GOOS))
		default:
			missing = append(missing, binary)
		}
	}

	var parts []string
	if len(missing) > 0 {
		parts = append(parts, fmt.Sprintf("missing on PATH: %v", missing))
	}

	return strings.Join(append(parts, refused...), "; ")
}

// refusedBinary names a tool LookPath refused, why, and where to install it instead.
func refusedBinary(binary string, err error, goos string) string {
	return fmt.Sprintf("found %s but refused it (%v). %s", binary, err, trustedInstallHint(goos))
}

func trustedInstallHint(goos string) string {
	switch goos {
	case "windows":
		return "Install it under Program Files, or another folder only Administrators and SYSTEM can change."
	case "darwin":
		return "Install it owned by root, mode 0755, in a folder only root can change (e.g. /usr/local/bin)."
	default:
		return "Install it owned by root, mode 0755, in a folder only root can change (e.g. /usr/local/bin)."
	}
}

func newCheck(name string, err error) service.PreflightCheck {
	if err != nil {
		return service.NewPreflightCheck(name, service.SeverityError, err.Error())
	}

	return service.NewPreflightCheck(name, service.SeverityOK, "")
}

func newPaymasterCheck(fallback bool, err error) service.PreflightCheck {
	if !fallback {
		return newCheck(paymasterCheckName, err)
	}

	return newWarning(
		paymasterCheckName,
		fmt.Sprintf("paymaster check failed: %v; the signer pays fees and rent when the paymaster fails", err),
	)
}

func newWarning(name, detail string) service.PreflightCheck {
	return service.NewPreflightCheck(name, service.SeverityWarning, detail)
}
