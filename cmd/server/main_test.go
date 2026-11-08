package main

import "testing"

func TestIsLoopback(t *testing.T) {
	for in, want := range map[string]bool{
		"127.0.0.1:8080": true, "localhost:8080": true, "localhost": true, "[::1]:8080": true, "[::1]": true,
		"127.0.0.2": true, ":8080": false, "0.0.0.0:8080": false, "evil.example:8080": false,
		"localhost.evil.example": false, "192.168.0.10:8080": false,
	} {
		if got := isLoopback(in); got != want {
			t.Errorf("isLoopback(%q) = %v, want %v", in, got, want)
		}
	}
}
