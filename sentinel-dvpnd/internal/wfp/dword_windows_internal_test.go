//go:build windows

package wfp

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// Pure logic: no WFP call is made. On arm64 the upper half of a DWORD return register is
// undefined, so only the low 32 bits may decide success and the error code.
func TestDwordResult_IgnoresTheUpperHalf(t *testing.T) {
	t.Parallel()

	const garbage = uintptr(0xdeadbeef) << 32

	require.NoError(t, dwordResult(0))
	require.NoError(t, dwordResult(garbage), "a success with junk in the upper half is still a success")

	err := dwordResult(garbage | uintptr(ErrSublayerNotFound))
	require.True(t, errors.Is(err, ErrSublayerNotFound), "the code must still match: %v", err)

	require.ErrorIs(t, dwordResult(uintptr(ErrFilterNotFound)), ErrFilterNotFound)
}
