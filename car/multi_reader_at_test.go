package car

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	mrand "math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiReaderAt(t *testing.T) {
	req := require.New(t)

	sourceData := make([]byte, 4<<20)
	_, err := rand.Read(sourceData)
	req.NoError(err)

	testRead := func(t *testing.T, mra io.ReaderAt, readLen int, pos int) {
		// t.Logf("testRead() readLen=%d pos=%d", readLen, pos)
		req := require.New(t)
		readData := make([]byte, readLen)
		n, err := mra.ReadAt(readData, int64(pos))
		req.NoError(err)
		req.Equal(readLen, n)
		req.True(bytes.Equal(sourceData[pos:pos+readLen], readData))
	}

	for _, testCase := range [][]int{
		{1},
		{8},
		{10},
		{1000},
		{1024},
		{2000},
		{1 << 20},
		{10, 10},
		{1 << 20, 1 << 20},
		{10, 10, 10},
		{1 << 20, 1 << 20, 1 << 20},
		{1, 1, 1, 1, 1},
		{8, 1, 8, 1, 8},
		{1000, 8, 10, 1000},
		{1000, 2000, 2000, 1000},
		{1000, 2000, 2000, 8, 1000},
		{8, 2000, 1024, 1 << 20, 1000},
	} {
		var sb strings.Builder
		for ii, sz := range testCase {
			if ii > 0 {
				sb.WriteString("_")
			}
			sb.WriteString(fmt.Sprintf("%d", sz))
		}

		t.Run(sb.String(), func(t *testing.T) {
			testLen := 0
			ra := make([]ReaderAtSize, len(testCase))
			for ii, sz := range testCase {
				ra[ii] = bytes.NewReader(sourceData[testLen : testLen+sz])
				testLen += sz
			}
			mra := NewMultiReaderAt(ra...)
			// read all
			testRead(t, mra, testLen, 0)
			// read at random positions
			for ii := 0; ii < 100; ii++ {
				pos := mrand.Intn(testLen)
				readLen := mrand.Intn(testLen - pos)
				testRead(t, mra, readLen, pos)
			}
			// read blocks
			off := 0
			for _, sz := range testCase {
				testRead(t, mra, sz, off)
				off += sz
			}
			// read just outsize of blocks
			off = 0
			for ii, sz := range testCase {
				pos := off
				rd := sz
				if ii > 0 {
					rd++
					off--
				}
				if off < testLen {
					rd++
				}
				if rd > testLen-pos {
					rd = testLen - pos
				}
				testRead(t, mra, rd, pos)
				off += sz
			}
		})
	}
}
