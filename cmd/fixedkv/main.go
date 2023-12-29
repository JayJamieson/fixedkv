package main

import (
	"fmt"

	fixedkv "github.com/JayJamieson/fixed-kv"
)

func main() {
	kv, err := fixedkv.NewFixedKV("fixedkv.db")

	if err != nil {
		fmt.Printf("%v\n", err)
	}

	defer kv.Close()
	fmt.Println("fixed-kv cli")
}
