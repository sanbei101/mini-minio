package cmd

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/sanbei101/mini-minio/internal/storage"
)

// ClusterConfig describes one node in a fixed distributed erasure deployment.
type ClusterConfig struct {
	NodeURL       string
	Endpoints     []string
	DataBlocks    int
	ParityBlocks  int
	ClusterSecret string
}

// NewCluster creates the object layer and the private storage RPC handler for
// one node. All nodes must use the same endpoint order and erasure settings.
func NewCluster(config ClusterConfig) (ObjectLayer, http.Handler, error) {
	if config.ClusterSecret == "" {
		return nil, nil, errors.New("cluster secret is required")
	}
	if config.NodeURL == "" || len(config.Endpoints) == 0 {
		return nil, nil, errors.New("node URL and endpoints are required")
	}

	node, err := url.Parse(config.NodeURL)
	if err != nil || node.Scheme == "" || node.Host == "" {
		return nil, nil, fmt.Errorf("invalid node URL %q", config.NodeURL)
	}

	configHash := clusterConfigHash(config)
	drives := make([]storage.API, len(config.Endpoints))
	localDrives := make(map[string]*storage.Disk)
	for index, endpointText := range config.Endpoints {
		endpoint, err := url.Parse(endpointText)
		if err != nil || endpoint.Scheme == "" || endpoint.Host == "" || endpoint.Path == "" {
			return nil, nil, fmt.Errorf("invalid endpoint %q", endpointText)
		}
		driveID := strconv.Itoa(index)
		if endpoint.Host == node.Host {
			disk, err := storage.NewDisk(endpoint.Path)
			if err != nil {
				return nil, nil, err
			}
			drives[index] = disk
			localDrives[driveID] = disk
			continue
		}
		drives[index] = newStorageRESTClient(endpoint, driveID, config.ClusterSecret, configHash)
	}

	objectLayer, err := NewErasureSetsWithDisks(drives, config.DataBlocks, config.ParityBlocks)
	if err != nil {
		return nil, nil, err
	}
	return objectLayer, NewStorageRESTHandler(localDrives, config.ClusterSecret, configHash), nil
}

func clusterConfigHash(config ClusterConfig) string {
	payload := fmt.Sprintf("%s\x00%d\x00%d",
		strings.Join(config.Endpoints, "\x00"),
		config.DataBlocks,
		config.ParityBlocks,
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}
