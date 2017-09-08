package main

import (
	"fmt"
	"github.com/wal-g/wal-g"
	"os"
)

type ZeroWriter struct {

}

func (b *ZeroWriter) Write(p []byte) (n int, err error) {
	return len(p),nil
}


func main() {
	id := walg.GetKeyRingId()
	fmt.Printf("Keyring ID: %v\n", id)
	var armour, err = walg.GetPubRingArmour(id)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Printf("Pubkey armour: %v\n", string(armour))

	armour, err = walg.GetSecretRingArmour(id)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Printf("Secret armour: %v\n", string(armour))

	var c walg.Crypter
	file, _ := os.Create("temp.txt")
	_,err = c.Encrypt(file)
	if err != nil {
		fmt.Println(err.Error())
	}

	err = c.Decrypt()
	if err != nil {
		fmt.Println(err.Error())
	}
}
