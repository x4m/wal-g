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

	return &DelayWriteCloser{writer, crypter.pubKey, nil}, nil
}

// Encryption starts writing header immidiately.
// But there is a lot of places where wrriter is instanciated long before pipe
// is ready. This is why here is used special writer, which delays encryption
// initialization before actual write. If no write occurs, initialization
// still is performed, to handle zero-byte files correctly
type DelayWriteCloser struct {
	inner io.WriteCloser
	el    openpgp.EntityList
	outer *io.WriteCloser
}

func (d *DelayWriteCloser) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if d.outer == nil {
		wc, err0 := openpgp.Encrypt(d.inner, d.el, nil, nil, nil)
		if err0 != nil {
			return 0, err
		}
		d.outer = &wc
	}
	n, err = (*d.outer).Write(p)
	return
}

func (d *DelayWriteCloser) Close() error {
	if d.outer == nil {
		wc, err0 := openpgp.Encrypt(d.inner, d.el, nil, nil, nil)
		if err0 != nil {
			return err0
		}
		d.outer = &wc
	}

	return (*d.outer).Close()
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
