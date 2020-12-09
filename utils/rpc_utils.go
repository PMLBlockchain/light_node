package utils

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

func QuerySelf(url string, rpc_data string) string {

	//url := "http://127.0.0.1:5000/"

	payload := strings.NewReader(rpc_data)

	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("content-type", "application/json")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)

	fmt.Println(res)
	fmt.Println(string(body))
	return string(body)
}
