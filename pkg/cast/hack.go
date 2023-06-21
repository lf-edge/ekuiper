package cast

import "unsafe"

func StringToBytes(str string) []byte {
	return unsafe.Slice(unsafe.StringData(str), len(str))
}
