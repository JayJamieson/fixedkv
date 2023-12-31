package main

import (
	"fmt"

	fixedkv "github.com/JayJamieson/fixed-kv"
)

func main() {
	kv, err := fixedkv.New("fixedkv.db", 3)

	if err != nil {
		fmt.Printf("%v\n", err)
	}

	kv.Set("A", []byte("A"))
	kv.Set("F", []byte("FFFFFF"))
	kv.Set("E", []byte("EEEEE"))
	kv.Set("D", []byte("DDDD"))
	kv.Set("C", []byte("CCC"))
	kv.Set("B", []byte("BB"))

	kv.Close()
	fmt.Println("fixed-kv cli")
}
