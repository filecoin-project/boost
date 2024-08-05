package extension

import (
	"errors"

	"github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/datatransfer/message"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/libp2p/go-libp2p/core/protocol"

	graphsync "github.com/filecoin-project/boost-graphsync"
)

const (
	// ExtensionIncomingRequest1_1 is the identifier for data sent by the IncomingRequest hook
	ExtensionIncomingRequest1_1 = graphsync.ExtensionName("fil/data-transfer/incoming-request/1.1")
	// ExtensionOutgoingBlock1_1 is the identifier for data sent by the OutgoingBlock hook
	ExtensionOutgoingBlock1_1 = graphsync.ExtensionName("fil/data-transfer/outgoing-block/1.1")
	// ExtensionDataTransfer1_1 is the identifier for the v1.1 data transfer extension to graphsync
	ExtensionDataTransfer1_1 = graphsync.ExtensionName("fil/data-transfer/1.1")
)

// ProtocolMap maps graphsync extensions to their libp2p protocols
var ProtocolMap = map[graphsync.ExtensionName]protocol.ID{
	ExtensionIncomingRequest1_1: datatransfer.ProtocolDataTransfer1_2,
	ExtensionOutgoingBlock1_1:   datatransfer.ProtocolDataTransfer1_2,
	ExtensionDataTransfer1_1:    datatransfer.ProtocolDataTransfer1_2,
}

// ToExtensionData converts a message to a graphsync extension
func ToExtensionData(msg datatransfer.Message, supportedExtensions []graphsync.ExtensionName) ([]graphsync.ExtensionData, error) {
	exts := make([]graphsync.ExtensionData, 0, len(supportedExtensions))
	for _, supportedExtension := range supportedExtensions {
		protoID, ok := ProtocolMap[supportedExtension]
		if !ok {
			return nil, errors.New("unsupported protocol")
		}
		versionedMsg, err := msg.MessageForProtocol(protoID)
		if err != nil {
			continue
		}
		nd, err := versionedMsg.ToIPLD()
		if err != nil {
			return nil, err
		}
		exts = append(exts, graphsync.ExtensionData{
			Name: supportedExtension,
			Data: nd,
		})
	}
	if len(exts) == 0 {
		return nil, errors.New("message not encodable in any supported extensions")
	}
	return exts, nil
}

// GsExtended is a small interface used by GetTransferData
type GsExtended interface {
	Extension(name graphsync.ExtensionName) (datamodel.Node, bool)
}

// GetTransferData unmarshals extension data.
// Returns:
//   - nil + nil if the extension is not found
//   - nil + error if the extendedData fails to unmarshal
//   - unmarshaled ExtensionDataTransferData + nil if all goes well
func GetTransferData(extendedData GsExtended, extNames []graphsync.ExtensionName) (datatransfer.Message, error) {
	for _, name := range extNames {
		data, ok := extendedData.Extension(name)
		if ok {
			return decoders[name](data)
		}
	}
	return nil, nil
}

type decoder func(datamodel.Node) (datatransfer.Message, error)

var decoders = map[graphsync.ExtensionName]decoder{
	ExtensionIncomingRequest1_1: message.FromIPLD,
	ExtensionOutgoingBlock1_1:   message.FromIPLD,
	ExtensionDataTransfer1_1:    message.FromIPLD,
}
