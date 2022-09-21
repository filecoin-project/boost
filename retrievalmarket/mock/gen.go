package mock

//go:generate go run github.com/golang/mock/mockgen -destination=./piecestore.go -package=mock github.com/filecoin-project/go-fil-markets/piecestore PieceStore
//go:generate go run github.com/golang/mock/mockgen -destination=./retrievalmarket.go -package=mock github.com/filecoin-project/go-fil-markets/retrievalmarket RetrievalProvider,SectorAccessor
