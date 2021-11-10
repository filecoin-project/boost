package datatransfer

import "encoding/json"

var TransferLocal = &transferLocalImpl{}

type TransferLocalParams struct {
	Path string
}

type transferLocalImpl struct {
}

func (t *transferLocalImpl) Type() string {
	return "local"
}

func (t *transferLocalImpl) MarshallParams(params *TransferLocalParams) ([]byte, error) {
	return json.Marshal(params)
}

func (t *transferLocalImpl) UnmarshallParams(params []byte) (*TransferLocalParams, error) {
	var paramsStruct TransferLocalParams
	err := json.Unmarshal(params, &paramsStruct)
	if err != nil {
		return nil, err
	}
	return &paramsStruct, nil
}
