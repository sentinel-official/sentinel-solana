package main

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	_ "github.com/sentinel-official/sentinel-go-sdk/v3/amneziawg"
	"github.com/sentinel-official/sentinel-go-sdk/v3/cli"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	_ "github.com/sentinel-official/sentinel-go-sdk/v3/hysteria2"
	_ "github.com/sentinel-official/sentinel-go-sdk/v3/openvpn"
	_ "github.com/sentinel-official/sentinel-go-sdk/v3/v2ray"
	_ "github.com/sentinel-official/sentinel-go-sdk/v3/wireguard"
	_ "github.com/sentinel-official/sentinel-go-sdk/v3/xray"
)

const keysDirName = "keys"

var errMissingInstance = errors.New("--instance is required")

type instanceOptions struct {
	home string
	tag  string
}

func (o *instanceOptions) load(flags *pflag.FlagSet) (*config.Loaded, error) {
	tag, err := o.parse()
	if err != nil {
		return nil, err
	}

	return config.Load(tag, o.home, flags)
}

func (o *instanceOptions) parse() (instance.Tag, error) {
	if o.tag == "" {
		return "", errMissingInstance
	}

	return instance.Parse(o.tag)
}

func (o *instanceOptions) resolve() (instance.Tag, string, error) {
	tag, err := o.parse()
	if err != nil {
		return "", "", err
	}

	dir, err := config.Dir(tag, o.home)
	if err != nil {
		return "", "", err
	}

	return tag, dir, nil
}

func main() {
	gin.SetMode(gin.ReleaseMode)
	os.Exit(cli.Run(newRootCmd()))
}

func newKeyCmd(opts *instanceOptions) *cobra.Command {
	var dir string

	cmd := cli.NewKeyCmd(func() string { return filepath.Join(dir, keysDirName) })
	cmd.PersistentPreRunE = func(*cobra.Command, []string) error {
		tag, err := opts.parse()
		if err != nil {
			return err
		}

		found, err := config.Find(tag, opts.home)
		if err != nil {
			return err
		}

		dir = found

		return nil
	}
	cmd.AddCommand(newKeySweepCmd(opts))

	return cmd
}

func newRootCmd() *cobra.Command {
	var opts instanceOptions

	root := &cobra.Command{
		Short: "Sentinel dVPN daemon",
		Use:   instance.DaemonName,
	}
	root.PersistentFlags().StringVar(&opts.home, "home", "", "instance folder override for tests and development")
	root.PersistentFlags().StringVar(&opts.tag, "instance", "", "daemon instance `tag`: c<N>, n<N> or p<N>")

	host := newServiceHost()

	root.AddCommand(
		newInitCmd(&opts),
		newRunCmd(&opts),
		newDoctorCmd(&opts, newKillSwitchProbe(), host),
		newKeyCmd(&opts),
		newServiceCmd(&opts, host),
		cli.NewCompletionCmd("dvpnd"),
		cli.NewVersionCmd(),
	)

	return root
}
