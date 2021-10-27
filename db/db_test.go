package db

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"path"
	"testing"
)

func TestDB(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	tmpFile := path.Join(t.TempDir(), "test.db")
	//fmt.Println(tmpFile)

	err := LoadFixtures(ctx, tmpFile, "create.sql", "fixtures.sql")
	req.NoError(err)

	db, err := Open(tmpFile)
	req.NoError(err)

	deals, err := db.Query(ctx)
	req.NoError(err)
	req.Len(deals, 2)

	fmt.Println(deals[0])
}
