package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/phuslu/log"

	"github.com/sanbei101/mini-minio/cmd"
)

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	dataDir := flag.String("data", "./data", "base data directory")
	data := flag.Int("data-blocks", 4, "number of data blocks")
	parity := flag.Int("parity-blocks", 2, "number of parity blocks")
	sets := flag.Int("sets", 1, "number of erasure sets")
	nodeURL := flag.String("node-url", "", "this node URL in cluster mode")
	var endpoints endpointFlags
	flag.Var(&endpoints, "endpoint", "cluster drive URL (repeat for every drive)")
	clusterSecret := flag.String("cluster-secret", os.Getenv("MINI_CLUSTER_SECRET"), "shared cluster storage secret")
	accessKey := flag.String("access-key", "", "S3 access key (empty = no auth)")
	secretKey := flag.String("secret-key", "", "S3 secret key")
	flag.Parse()
	log.DefaultLogger = log.Logger{
		Level:  log.InfoLevel,
		Caller: 0,
		Writer: &log.IOWriter{Writer: os.Stderr},
	}
	var router http.Handler
	if len(endpoints) > 0 {
		if *nodeURL == "" {
			log.Fatal().Msg("--node-url is required with --endpoint")
		}
		obj, storageHandler, err := cmd.NewCluster(cmd.ClusterConfig{
			NodeURL:       *nodeURL,
			Endpoints:     endpoints,
			DataBlocks:    *data,
			ParityBlocks:  *parity,
			ClusterSecret: *clusterSecret,
		})
		if err != nil {
			log.Fatal().Err(err)
		}
		mux := http.NewServeMux()
		mux.Handle(cmd.StorageRESTPrefix(), storageHandler)
		mux.Handle("/", cmd.NewRouter(obj, cmd.Credentials{AccessKey: *accessKey, SecretKey: *secretKey}))
		router = mux
	} else {
		total := (*data + *parity) * *sets
		diskPaths := make([]string, total)
		for i := range diskPaths {
			diskPaths[i] = filepath.Join(*dataDir, fmt.Sprintf("disk%d", i))
			if err := os.MkdirAll(diskPaths[i], 0o755); err != nil {
				log.Fatal().Err(err)
			}
		}
		log.Info().Strs("disks", diskPaths).Msg("initialized disks")
		obj, err := cmd.NewErasureSets(diskPaths, *data, *parity)
		if err != nil {
			log.Fatal().Err(err)
		}
		router = cmd.NewRouter(obj, cmd.Credentials{AccessKey: *accessKey, SecretKey: *secretKey})
	}
	log.Info().Str("addr", *addr).Msg("starting server")
	log.Fatal().Err(http.ListenAndServe(*addr, router))
}

type endpointFlags []string

func (f *endpointFlags) String() string { return strings.Join(*f, ",") }

func (f *endpointFlags) Set(value string) error {
	*f = append(*f, value)
	return nil
}
