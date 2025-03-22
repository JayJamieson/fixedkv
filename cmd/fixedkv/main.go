package main

import (
	"fmt"
	"os"

	fixedkv "github.com/JayJamieson/fixedkv"
)

func main() {
	os.Remove("fixedkv.db")
	kv, err := fixedkv.Open("fixedkv.db")

	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	kv.Set("A", []byte("Aa"))
	kv.Set("F", []byte("FFFFFF"))
	kv.Set("E", []byte("EEEEE"))
	kv.Set("D", []byte("DDDD"))
	kv.Set("C", []byte("CCC"))
	kv.Set("B", []byte("BB"))

	kv.Save()
	kv.Close()
	fmt.Println("fixed-kv cli")

	db, _ := fixedkv.OpenReader("fixedkv.db")

	val, _ := db.Get("A")
	fmt.Println(string(val))

	val, _ = db.Get("F")
	fmt.Println(string(val))

}
