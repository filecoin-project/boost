package encoding

import (
	"bytes"
	"reflect"

	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	cborgen "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

// Encodable is an object that can be written to CBOR and decoded back
type Encodable interface{}

// Encode encodes an encodable to CBOR, using the best available path for
// writing to CBOR
func Encode(value Encodable) ([]byte, error) {
	if cbgEncodable, ok := value.(cborgen.CBORMarshaler); ok {
		buf := new(bytes.Buffer)
		err := cbgEncodable.MarshalCBOR(buf)
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	if ipldEncodable, ok := value.(datamodel.Node); ok {
		if tn, ok := ipldEncodable.(schema.TypedNode); ok {
			ipldEncodable = tn.Representation()
		}
		buf := &bytes.Buffer{}
		err := dagcbor.Encode(ipldEncodable, buf)
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	return cbor.DumpObject(value)
}

func EncodeToNode(encodable Encodable) (datamodel.Node, error) {
	byts, err := Encode(encodable)
	if err != nil {
		return nil, err
	}
	na := basicnode.Prototype.Any.NewBuilder()
	if err := dagcbor.Decode(na, bytes.NewReader(byts)); err != nil {
		return nil, err
	}
	return na.Build(), nil
}

// Decoder is CBOR decoder for a given encodable type
type Decoder interface {
	DecodeFromCbor([]byte) (Encodable, error)
	DecodeFromNode(datamodel.Node) (Encodable, error)
}

// NewDecoder creates a new Decoder that will decode into new instances of the given
// object type. It will use the decoding that is optimal for that type
// It returns error if it's not possible to setup a decoder for this type
func NewDecoder(decodeType Encodable) (Decoder, error) {
	// check if type is datamodel.Node, if so, just use style
	if ipldDecodable, ok := decodeType.(datamodel.Node); ok {
		return &ipldDecoder{ipldDecodable.Prototype()}, nil
	}
	// check if type is a pointer, as we need that to make new copies
	// for cborgen types & regular IPLD types
	decodeReflectType := reflect.TypeOf(decodeType)
	if decodeReflectType.Kind() != reflect.Ptr {
		return nil, xerrors.New("type must be a pointer")
	}
	// check if type is a cbor-gen type
	if _, ok := decodeType.(cborgen.CBORUnmarshaler); ok {
		return &cbgDecoder{decodeReflectType}, nil
	}
	// type does is neither ipld-prime nor cbor-gen, so we need to see if it
	// can rountrip with oldschool ipld-format
	encoded, err := cbor.DumpObject(decodeType)
	if err != nil {
		return nil, xerrors.New("Object type did not encode")
	}
	newDecodable := reflect.New(decodeReflectType.Elem()).Interface()
	if err := cbor.DecodeInto(encoded, newDecodable); err != nil {
		return nil, xerrors.New("Object type did not decode")
	}
	return &defaultDecoder{decodeReflectType}, nil
}

type ipldDecoder struct {
	style ipld.NodePrototype
}

func (decoder *ipldDecoder) DecodeFromCbor(encoded []byte) (Encodable, error) {
	builder := decoder.style.NewBuilder()
	buf := bytes.NewReader(encoded)
	err := dagcbor.Decode(builder, buf)
	if err != nil {
		return nil, err
	}
	return builder.Build(), nil
}

func (decoder *ipldDecoder) DecodeFromNode(node datamodel.Node) (Encodable, error) {
	builder := decoder.style.NewBuilder()
	if err := builder.AssignNode(node); err != nil {
		return nil, err
	}
	return builder.Build(), nil
}

type cbgDecoder struct {
	cbgType reflect.Type
}

func (decoder *cbgDecoder) DecodeFromCbor(encoded []byte) (Encodable, error) {
	decodedValue := reflect.New(decoder.cbgType.Elem())
	decoded, ok := decodedValue.Interface().(cborgen.CBORUnmarshaler)
	if !ok || reflect.ValueOf(decoded).IsNil() {
		return nil, xerrors.New("problem instantiating decoded value")
	}
	buf := bytes.NewReader(encoded)
	err := decoded.UnmarshalCBOR(buf)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

func (decoder *cbgDecoder) DecodeFromNode(node datamodel.Node) (Encodable, error) {
	if tn, ok := node.(schema.TypedNode); ok {
		node = tn.Representation()
	}
	buf := &bytes.Buffer{}
	if err := dagcbor.Encode(node, buf); err != nil {
		return nil, err
	}
	return decoder.DecodeFromCbor(buf.Bytes())
}

type defaultDecoder struct {
	ptrType reflect.Type
}

func (decoder *defaultDecoder) DecodeFromCbor(encoded []byte) (Encodable, error) {
	decodedValue := reflect.New(decoder.ptrType.Elem())
	decoded, ok := decodedValue.Interface().(Encodable)
	if !ok || reflect.ValueOf(decoded).IsNil() {
		return nil, xerrors.New("problem instantiating decoded value")
	}
	err := cbor.DecodeInto(encoded, decoded)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

func (decoder *defaultDecoder) DecodeFromNode(node datamodel.Node) (Encodable, error) {
	if tn, ok := node.(schema.TypedNode); ok {
		node = tn.Representation()
	}
	buf := &bytes.Buffer{}
	if err := dagcbor.Encode(node, buf); err != nil {
		return nil, err
	}
	return decoder.DecodeFromCbor(buf.Bytes())
}
