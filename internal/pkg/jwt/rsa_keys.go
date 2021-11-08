package jwt

import (
	"crypto/rsa"
	"fmt"
	"github.com/golang-jwt/jwt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"io/ioutil"
	"path"
	"strings"
	"sync"
)

var privateKeyRepository = make(map[string]*rsa.PrivateKey)
var repositoryLock sync.Mutex

const RSAKeyDir = "mgmt"

func GetPrivateKeyWithKeyName(keyName string) (*rsa.PrivateKey, error) {
	repositoryLock.Lock()
	defer repositoryLock.Unlock()

	key, ok := privateKeyRepository[keyName]
	if ok {
		return key, nil
	}

	privateKey, err := privateKeyFromFile(keyName)
	if err != nil {
		return nil, err
	}

	privateKeyRepository[keyName] = privateKey

	return privateKey, nil
}

func GetPublicKey(keyName string) (*rsa.PublicKey, error) {
	publicKey, err := publicKeyFromFile(keyName)
	if err != nil {
		return nil, err
	}

	return publicKey, nil
}

func insensitiveGetFilePath(prikeyName string) (string, error) {
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return "", err
	}

	dir := path.Join(confDir, RSAKeyDir)
	infos, err := ioutil.ReadDir(dir)
	if nil != err {
		return "", err
	}

	for _, info := range infos {
		fileName := info.Name()
		if strings.ToLower(fileName) == strings.ToLower(prikeyName) {
			filePath := path.Join(dir, fileName)
			return filePath, nil
		}
	}
	return "", fmt.Errorf("not found target key file %s in /etc/%s", prikeyName, RSAKeyDir)
}

func privateKeyFromFile(keyName string) (*rsa.PrivateKey, error) {
	keyPath, err := insensitiveGetFilePath(keyName)
	if err != nil {
		return nil, err
	}
	keyBytes, err := ioutil.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	signKey, err := jwt.ParseRSAPrivateKeyFromPEM(keyBytes)
	if err != nil {
		return nil, err
	}
	return signKey, nil
}

func publicKeyFromFile(keyName string) (*rsa.PublicKey, error) {
	keyPath, err := insensitiveGetFilePath(keyName)
	if err != nil {
		return nil, err
	}
	keyBytes, err := ioutil.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	pubKey, err := jwt.ParseRSAPublicKeyFromPEM(keyBytes)
	if err != nil {
		return nil, err
	}
	return pubKey, nil
}
