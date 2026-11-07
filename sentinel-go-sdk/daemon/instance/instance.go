package instance

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"strings"
)

const (
	DaemonName  = "sentinel-dvpnd"
	GroupName   = ProductName
	ProductName = "sentinel-dvpn"

	RoleClient   = "client"
	RoleNode     = "node"
	RoleProvider = "provider"
)

const socketFileName = "daemon.sock"

var (
	ErrInstanceNotFound = errors.New("instance not found")
	ErrInvalidTag       = errors.New("invalid instance tag")
)

var tagPattern = regexp.MustCompile(`^[cnp](0|[1-9][0-9]{0,8})$`)

type NotFoundError struct {
	available []Tag
	tag       Tag
}

func NewNotFoundError(t Tag, available []Tag) error {
	return &NotFoundError{
		available: available,
		tag:       t,
	}
}

func (e *NotFoundError) Error() string {
	available := "none"

	if len(e.available) > 0 {
		names := make([]string, 0, len(e.available))
		for _, tag := range e.available {
			names = append(names, string(tag))
		}

		available = strings.Join(names, ", ")
	}

	return fmt.Sprintf("instance %s not found; available: %s", e.tag, available)
}

func (e *NotFoundError) Unwrap() error {
	return ErrInstanceNotFound
}

type Tag string

func (t Tag) Endpoint(goos string) string {
	switch goos {
	case "windows":
		return `\\.\pipe\ProtectedPrefix\Administrators\` + ProductName + `\` + string(t)
	case "linux":
		// Under /run, not in the state folder: SELinux labels a new file from its path, and under
		// /var/lib that is var_lib_t, which systemd (init_t) may not create a socket as. The
		// socket unit then never came up on Fedora and EL (enforcing). /run is var_run_t, the type
		// of every socket systemd makes there, with no extra policy or package.
		return RuntimeDir(goos, t) + "/" + socketFileName
	default:
		return t.StateDir(goos) + "/" + socketFileName
	}
}

// RuntimeDir is the Linux folder for an instance's runtime files (its control socket).
func RuntimeDir(_ string, t Tag) string {
	return "/run/" + ProductName + "/" + string(t)
}

func (t Tag) EnvPath() string {
	return "/etc/" + ProductName + "/" + string(t) + ".env"
}

func (t Tag) Interface() string {
	return "dvpn-" + string(t)
}

func (t Tag) LogPath(goos string) string {
	switch goos {
	case "darwin":
		return "/Library/Logs/" + ProductName + "/" + string(t) + ".log"
	case "windows":
		return t.StateDir(goos) + `\daemon.log`
	default:
		return ""
	}
}

func (t Tag) ObjectName() string {
	return ProductName + "-" + string(t)
}

func (t Tag) Role() string {
	if !tagPattern.MatchString(string(t)) {
		return ""
	}

	switch t[0] {
	case 'c':
		return RoleClient
	case 'n':
		return RoleNode
	default:
		return RoleProvider
	}
}

func (t Tag) ServiceName(goos string) string {
	switch goos {
	case "darwin":
		return "co.sentinel." + DaemonName + "." + string(t)
	case "windows":
		return DaemonName + "$" + string(t)
	default:
		return DaemonName + "@" + string(t)
	}
}

func (t Tag) StateDir(goos string) string {
	if goos == "windows" {
		return StateRoot(goos) + `\` + string(t)
	}

	return StateRoot(goos) + "/" + string(t)
}

func List(root string) ([]Tag, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("reading instance root: %w", err)
	}

	var tags []Tag

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		tag, err := Parse(entry.Name())
		if err != nil {
			continue
		}

		tags = append(tags, tag)
	}

	return tags, nil
}

func Parse(s string) (Tag, error) {
	if !tagPattern.MatchString(s) {
		return "", fmt.Errorf("%w: %q", ErrInvalidTag, s)
	}

	return Tag(s), nil
}

func StateRoot(goos string) string {
	switch goos {
	case "darwin":
		return "/Library/Application Support/" + ProductName
	case "windows":
		programData := os.Getenv("ProgramData")
		if programData == "" {
			programData = `C:\ProgramData`
		}

		return programData + `\` + ProductName
	default:
		return "/var/lib/" + ProductName
	}
}
