package remoteblockstore

import (
	"fmt"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNormalizeError(t *testing.T) {
	dummyCid, err := cid.Parse("bafkqaaa")
	require.NoError(t, err)

	testCases := []struct {
		name          string
		err           error
		expected      error
		isNotFoundErr bool
	}{{
		name:          "ipld ErrNotFound",
		err:           format.ErrNotFound{Cid: dummyCid},
		expected:      format.ErrNotFound{Cid: dummyCid},
		isNotFoundErr: true,
	}, {
		name:          "stringified ipld ErrNotFound",
		err:           fmt.Errorf(format.ErrNotFound{Cid: dummyCid}.Error()),
		expected:      format.ErrNotFound{Cid: dummyCid},
		isNotFoundErr: true,
	}, {
		name:          "ipld ErrNotFound no cid",
		err:           format.ErrNotFound{},
		expected:      format.ErrNotFound{},
		isNotFoundErr: true,
	}, {
		name:          "stringified ipld ErrNotFound no cid",
		err:           fmt.Errorf(format.ErrNotFound{}.Error()),
		expected:      format.ErrNotFound{},
		isNotFoundErr: true,
	}, {
		name:          "ipld ErrorNotFound with prefix",
		err:           fmt.Errorf("some err: %w", format.ErrNotFound{Cid: dummyCid}),
		expected:      fmt.Errorf("some err: %w", format.ErrNotFound{Cid: dummyCid}),
		isNotFoundErr: true,
	}, {
		name:          "stringified ipld ErrorNotFound with prefix",
		err:           fmt.Errorf(fmt.Errorf("some err: %w", format.ErrNotFound{Cid: dummyCid}).Error()),
		expected:      fmt.Errorf("some err: %w", format.ErrNotFound{Cid: dummyCid}),
		isNotFoundErr: true,
	}, {
		name:          "ipld ErrorNotFound no cid with prefix",
		err:           fmt.Errorf("some err: %w", format.ErrNotFound{}),
		expected:      fmt.Errorf("some err: %w", format.ErrNotFound{}),
		isNotFoundErr: true,
	}, {
		name:          "stringified ipld ErrorNotFound no cid with prefix",
		err:           fmt.Errorf(fmt.Errorf("some err: %w", format.ErrNotFound{}).Error()),
		expected:      fmt.Errorf("some err: %w", format.ErrNotFound{}),
		isNotFoundErr: true,
	}, {
		name:          "block not found",
		err:           fmt.Errorf("block not found"),
		expected:      format.ErrNotFound{},
		isNotFoundErr: true,
	}, {
		name:          "different capitalization not found",
		err:           fmt.Errorf("block nOt FoUnd"),
		expected:      format.ErrNotFound{},
		isNotFoundErr: true,
	}, {
		name:          "different error",
		err:           fmt.Errorf("some other err"),
		expected:      fmt.Errorf("some other err"),
		isNotFoundErr: false,
	}, {
		name:          "stringified ipld ErrorNotFound with bad cid",
		err:           fmt.Errorf(ipldNotFoundPrefix + "badcid"),
		expected:      fmt.Errorf(ipldNotFoundPrefix + "badcid"),
		isNotFoundErr: false,
	}, {
		name:          "nil error",
		err:           nil,
		expected:      nil,
		isNotFoundErr: false,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := normalizeError(tc.err)
			if tc.isNotFoundErr {
				require.True(t, format.IsNotFound(err))
			} else {
				require.False(t, format.IsNotFound(err))
			}
			if tc.err == nil {
				require.Nil(t, err)
			} else {
				require.Equal(t, tc.expected.Error(), err.Error())
			}
		})
	}
}
