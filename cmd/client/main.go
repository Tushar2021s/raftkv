package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	server := flag.String("server", "localhost:8000", "host:port of any cluster node")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	base := "http://" + *server
	client := &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return nil // follow leader redirects automatically
		},
	}

	switch strings.ToLower(args[0]) {
	case "get":
		if len(args) < 2 {
			die("usage: client get <key>")
		}
		doGet(client, base, args[1])

	case "put":
		if len(args) < 3 {
			die("usage: client put <key> <value>")
		}
		doPut(client, base, args[1], args[2])

	case "delete":
		if len(args) < 2 {
			die("usage: client delete <key>")
		}
		doDelete(client, base, args[1])

	case "status":
		doStatus(client, base)

	default:
		printUsage()
		os.Exit(1)
	}
}

func doGet(c *http.Client, base, key string) {
	resp, err := c.Get(base + "/kv/" + key)
	mustOK(err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		var r map[string]string
		json.Unmarshal(body, &r)
		fmt.Println(r["value"])
	} else {
		fmt.Fprintf(os.Stderr, "error %d: %s\n", resp.StatusCode, strings.TrimSpace(string(body)))
		os.Exit(1)
	}
}

func doPut(c *http.Client, base, key, value string) {
	// Generate a simple reqId from timestamp for the CLI (a real client
	// library would use a UUID and store it for retry).
	reqID := fmt.Sprintf("cli-%d", time.Now().UnixNano())
	payload, _ := json.Marshal(map[string]string{"value": value, "reqId": reqID})
	req, _ := http.NewRequest(http.MethodPut, base+"/kv/"+key, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	mustOK(err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		fmt.Println("OK")
	} else {
		fmt.Fprintf(os.Stderr, "error %d: %s\n", resp.StatusCode, strings.TrimSpace(string(body)))
		os.Exit(1)
	}
}

func doDelete(c *http.Client, base, key string) {
	reqID := fmt.Sprintf("cli-%d", time.Now().UnixNano())
	payload, _ := json.Marshal(map[string]string{"reqId": reqID})
	req, _ := http.NewRequest(http.MethodDelete, base+"/kv/"+key, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	mustOK(err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		fmt.Println("OK")
	} else {
		fmt.Fprintf(os.Stderr, "error %d: %s\n", resp.StatusCode, strings.TrimSpace(string(body)))
		os.Exit(1)
	}
}

func doStatus(c *http.Client, base string) {
	resp, err := c.Get(base + "/status")
	mustOK(err)
	defer resp.Body.Close()
	var r map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&r)
	fmt.Printf("nodeId=%v  state=%v  term=%v  leader=%v\n",
		r["nodeId"], r["state"], r["term"], r["leader"])
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `raftkv client

Commands:
  get    <key>           read a key
  put    <key> <value>   write a key
  delete <key>           delete a key
  status                 show node state

Flags:
  -server host:port      any node in the cluster (default localhost:8000)`)
}

func mustOK(err error) {
	if err != nil {
		die(err.Error())
	}
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
