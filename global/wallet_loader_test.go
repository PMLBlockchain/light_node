package global

import (
	"fmt"
	"testing"
)

func TestGetPassword(t *testing.T) {
	res, iv := GetPassword(190)
	fmt.Println(res, iv)
}
