package global

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/PMLBlockchain/light_node/hx_sdk"
	"github.com/PMLBlockchain/light_node/hx_sdk/common"
	"github.com/PMLBlockchain/light_node/utils"
	"io/ioutil"
	"math/rand"
	"time"

	"os"
	"strconv"
)

type ServerConfig struct {
	PasswordOrder int64             `json:"password,omitempty"` //wallet wif key sercet
	EncryptDatas  string            `json:"encryptDatas"`       // wallet wif encrypt datas
	MainAddress   string            `json:"mainAddress"`        // wallet master address
	WifKey        map[string]string `json:"-"`                  // wallet wif key
	MainPublicKey string            `json:"mainPublicKey"`
	ChainId       string            `json:chainId`
}

func NewServerConfig() ServerConfig {
	serverConfig := ServerConfig{
		PasswordOrder: 0,
		EncryptDatas:  "",
		MainAddress:   "",
		WifKey:        make(map[string]string, 0),
		MainPublicKey: "",
		ChainId:       "2cce06920c16a2d4234df882aad28059802c4f77c956d999328dc740f37e183d",
	}
	return serverConfig
}

func PathExists(path string) (bool, error) {

	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func GetPassword(index int64) (string, []byte) {

	password := common.HashSha256([]byte("light_node_rand_seed"))
	password = append(password, []byte(strconv.FormatInt(index, 10))...)
	password_end := common.HashSha256(password)

	password_str := base64.StdEncoding.EncodeToString(password_end[:16])
	iv := password_end[16:32]
	return password_str, iv

}

func LoadOrCreateWalletFile(path string) (*ServerConfig, error) {
	serverConfig := NewServerConfig()
	exist, err := PathExists(path)
	if err != nil {
		return nil, err
	}
	if exist {
		//文件存在，加载文件
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		fileData, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(fileData, &serverConfig)
		if err != nil {
			return nil, err
		}
		password, iv := GetPassword(serverConfig.PasswordOrder)
		baseData, err := base64.StdEncoding.DecodeString(serverConfig.EncryptDatas)
		if err != nil {
			return nil, err
		}
		jsonDatas, err := utils.AesDecrypt(baseData, []byte(password), iv, utils.PKCS5UnPadding)

		err = json.Unmarshal(jsonDatas, &serverConfig.WifKey)
		if err != nil {
			return nil, err
		}
		fmt.Println(serverConfig.WifKey)
		return &serverConfig, nil
	} else {
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0777)
		if err != nil {
			return nil, err
		}
		wif, pub, addr, err := hx_sdk.GetNewPrivate()
		if err != nil {
			return nil, err
		}
		serverConfig.MainAddress = addr
		serverConfig.MainPublicKey = pub
		serverConfig.WifKey[addr] = wif
		serverConfig.PasswordOrder = rand.New(rand.NewSource(time.Now().UnixNano())).Int63()
		password, iv := GetPassword(serverConfig.PasswordOrder)
		sourceData, err := json.Marshal(serverConfig.WifKey)
		if err != nil {
			return nil, err
		}
		encryptData, err := utils.AesEncrypt(sourceData, []byte(password), iv, utils.PKCS5Padding)
		serverConfig.EncryptDatas = base64.StdEncoding.EncodeToString(encryptData)
		fileData, err := json.Marshal(serverConfig)
		if err != nil {
			return nil, err
		}

		file.Write(fileData)
		defer file.Close()
	}
	return &serverConfig, nil
}
