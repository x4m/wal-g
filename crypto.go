package walg

import (
	"os"
	"golang.org/x/crypto/openpgp"
	"os/exec"
	"bytes"
	"io"
)

type Crypter struct {
	configured, armed bool
	keyRingId         string

	pubKey    openpgp.EntityList
	secretKey openpgp.EntityList
}

func (crypter *Crypter) IsUsed() bool {
	if !crypter.configured {
		crypter.Configure()
	}
	return crypter.armed
}

func (crypter *Crypter) Configure() {
	crypter.configured = true
	crypter.keyRingId = GetKeyRingId()
	crypter.armed = len(crypter.keyRingId) != 0
}

func (crypter *Crypter) Encrypt(writer io.WriteCloser) (io.WriteCloser, error) {
	if crypter.pubKey == nil {
		armour, err := GetPubRingArmour(crypter.keyRingId)
		if err != nil {
			return nil, err;
		}

		entitylist, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armour))
		if err != nil {
			return nil, err
		}
		crypter.pubKey = entitylist
	}

	var wc, err0 = openpgp.Encrypt(writer, crypter.pubKey, nil, nil, nil)
	if err0 != nil {
		return nil, err0;
	}

	return wc, nil
}

func (crypter *Crypter) Decrypt(reader io.ReadCloser) (io.Reader, error) {
	if crypter.secretKey == nil {
		armour, err := GetSecretRingArmour(crypter.keyRingId)
		if err != nil {
			return nil, err;
		}

		entitylist, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armour))
		if err != nil {
			return nil, err
		}
		crypter.secretKey = entitylist
	}

	var md, err0 = openpgp.ReadMessage(reader, crypter.secretKey, nil, nil)
	if err0 != nil {
		return nil, err0;
	}

	return md.UnverifiedBody, nil
}

func GetKeyRingId() string {
	return os.Getenv("WALE_GPG_KEY_ID")
}

const gpgBin = "gpg"

func GetPubRingArmour(keyId string) ([]byte, error) {
	out, err := exec.Command(gpgBin, "-a", "--export", "-r", "\""+keyId+"\"").Output()
	if err != nil {
		return nil, err
	}
	return out, nil
}

func GetSecretRingArmour(keyId string) ([]byte, error) {
	out, err := exec.Command(gpgBin, "-a", "--export-secret-key", "-r", "\""+keyId+"\"").Output()
	if err != nil {
		return nil, err
	}
	return out, nil
}
