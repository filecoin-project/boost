package config

import (
	"bytes"
	"fmt"

	"github.com/BurntSushi/toml"
)

func RemoveRedundantFieldsV1toV2(cfgCur interface{}) ([]byte, error) {
	var nodeStr string
	{
		buf := new(bytes.Buffer)
		e := toml.NewEncoder(buf)
		if err := e.Encode(cfgCur); err != nil {
			return nil, fmt.Errorf("encoding node config: %w", err)
		}

		nodeStr = buf.String()
	}

	return []byte(nodeStr), nil
}
