package main

import (
	"fmt"

	fixedkv "github.com/JayJamieson/fixedkv"
)

func main() {
	kv, err := fixedkv.New("fixedkv.db")

	if err != nil {
		fmt.Printf("%v\n", err)
	}

	kv.Set("A", []byte("Aa"))
	kv.Set("F", []byte("FFFFFF"))
	kv.Set("E", []byte("EEEEE"))
	kv.Set("D", []byte("DDDD"))
	kv.Set("C", []byte("CCC"))
	kv.Set("B", []byte("BB"))

	kv.Close()
	fmt.Println("fixed-kv cli")

	db, _ := fixedkv.Open("fixedkv.db")

	val, _ := db.Get("A")
	fmt.Println(string(val))

	values := db.Values()
	values[5][1] = 69
	fmt.Printf("%v\n", values)

	val, _ = db.Get("F")
	fmt.Println(string(val))

}
