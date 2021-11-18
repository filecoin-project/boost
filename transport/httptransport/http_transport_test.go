package httptransport

import (
	"testing"
)

func TestValidation(t *testing.T) {

}

/*
func TestSimpleTransfer(t *testing.T) {
	dealSize := int64(11999)

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, expected)
	}))

	ctx := context.Background()
	ht := &httpTransport{}
	dealInfo := &types.TransportDealInfo{}
	ti := &pb.HttpReq{
		URL: svr.URL,
	}
	bz, err := proto.Marshal(ti)
	require.NoError(t, err)
	dir := t.TempDir()
	of, err := os.CreateTemp(dir, "")
	require.NoError(t, of.Close())
	require.NoError(t, err)

	th, ic, err := ht.Execute(ctx, bz, &types.TransportDealInfo{
		OutputFile: of.Name(),
		DealSize:   dealSize,
	})
	require.NoError(t, err)
	require.False(t, ic)

	sub := th.Sub()
	var evts []types.TransportEvent

	for {
		select {
			case ev :<- sub.Out():

		}
	}
}

func startHttpServer(t *testing.T) {

}
*/
