package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/phuslu/log"

	"github.com/sanbei101/mini-minio/cmd"
)

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	dataDir := flag.String("data", "./data", "base data directory")
	data := flag.Int("data-blocks", 4, "number of data blocks")
	parity := flag.Int("parity-blocks", 2, "number of parity blocks")
	sets := flag.Int("sets", 1, "number of erasure sets")
	accessKey := flag.String("access-key", "", "S3 access key (empty = no auth)")
	secretKey := flag.String("secret-key", "", "S3 secret key")
	flag.Parse()
	log.DefaultLogger = log.Logger{
		Level:  log.InfoLevel,
		Caller: 0,
		Writer: &log.IOWriter{Writer: os.Stderr},
	}
	total := (*data + *parity) * *sets
	diskPaths := make([]string, total)
	for i := range diskPaths {
		diskPaths[i] = filepath.Join(*dataDir, fmt.Sprintf("disk%d", i))
		if err := os.MkdirAll(diskPaths[i], 0o755); err != nil {
			log.Fatal().Err(err)
		}
	}

	log.Info().Strs("disks", diskPaths).Msg("initialized disks")

	obj, err := cmd.NewErasureObjects(diskPaths, *data, *parity)
	if err != nil {
		log.Fatal().Err(err)
	}

	router := cmd.NewRouter(obj, cmd.Credentials{AccessKey: *accessKey, SecretKey: *secretKey})
	log.Info().Str("addr", *addr).Msg("starting server")
	log.Fatal().Err(http.ListenAndServe(*addr, router))
}
