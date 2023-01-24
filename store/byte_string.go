package store

import "encoding/base64"

type ByteString []byte

func (bs *ByteString) B64() string {
	return base64.StdEncoding.EncodeToString(*bs)
}


func ByteStringFromB64(k string) ByteString {
	kb, err := base64.StdEncoding.DecodeString(k)

	if err == nil {
		return kb
	}
	return nil
}
