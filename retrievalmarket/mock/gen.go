package mock

//go:generate go run github.com/golang/mock/mockgen -destination=./piecestore.go -package=mock github.com/filecoin-project/boost/node/modules/piecestore PieceStore
//go:generate go run github.com/golang/mock/mockgen -destination=./retrievalmarket.go -package=mock github.com/filecoin-project/Boost/retrievalmarket/legacyretrievaltypes RetrievalProvider,SectorAccessor
