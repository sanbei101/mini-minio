package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/sanbei101/mini-minio/cmd"
)

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	dataDir := flag.String("data", "./data", "base data directory")
	data := flag.Int("data-blocks", 4, "number of data blocks")
	parity := flag.Int("parity-blocks", 2, "number of parity blocks")
	accessKey := flag.String("access-key", "", "S3 access key (empty = no auth)")
	secretKey := flag.String("secret-key", "", "S3 secret key")
	flag.Parse()

	total := *data + *parity
	diskPaths := make([]string, total)
	for i := range diskPaths {
		diskPaths[i] = filepath.Join(*dataDir, fmt.Sprintf("disk%d", i))
		if err := os.MkdirAll(diskPaths[i], 0o755); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("disks: %s", strings.Join(diskPaths, ", "))

	obj, err := cmd.NewErasureObjects(diskPaths, *data, *parity)
	if err != nil {
		log.Fatal(err)
	}

	router := cmd.NewRouter(obj, cmd.Credentials{AccessKey: *accessKey, SecretKey: *secretKey})
	log.Printf("mini-minio listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, router))
}
