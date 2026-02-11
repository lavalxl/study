package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func usage() {
	fmt.Println("Usage:")
	fmt.Println("  client GET http://localhost:8080/data/1")
	fmt.Println("  client PUT http://localhost:8080/data/1 '{\"value\":\"11\"}'")
	fmt.Println("  client PUT http://localhost:8080/data/ '{\"2\":\"22\",\"3\":\"33\"}'")
	fmt.Println("  client DELETE http://localhost:8080/data/1")
	fmt.Println("  client WHOIS http://localhost:8080")
}

func main() {
	if len(os.Args) < 3 {
		usage()
		return
	}
	method := os.Args[1]
	url := os.Args[2]

	if method == "WHOIS" {
		if !strings.HasSuffix(url, "/") {
			url += "/"
		}
		resp, err := http.Get(url + "whois_master")
		if err != nil {
			fmt.Println("HTTP error:", err)
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		fmt.Printf("Status: %s\nResponse: %s\n", resp.Status, string(b))
		return
	}

	if method == "CAS" {
		method = "POST"
	}

	// Тело запроса PUT/POST
	var body io.Reader
	if len(os.Args) > 3 {
		body = bytes.NewReader([]byte(os.Args[3]))
	}

	client := &http.Client{
		// nil по умолчанию - follow all redirects чтобы GET сразу давал данные
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		fmt.Println("Request error:", err)
		return
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("HTTP error:", err)
		return
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	fmt.Printf("Status: %s\nResponse: %s\n", resp.Status, string(b))
}
