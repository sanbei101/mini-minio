package cmd

import (
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/phuslu/log"

	"github.com/sanbei101/mini-minio/internal/storage"
)

// NewStorageRESTHandler serves fixed local drives to authenticated cluster peers.
func NewStorageRESTHandler(drives map[string]*storage.Disk, secret, configHash string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validStorageRESTRequest(r, secret, configHash) {
			http.Error(w, "invalid internal storage request", http.StatusForbidden)
			return
		}
		relative := strings.TrimPrefix(r.URL.Path, storageRESTPrefix)
		parts := strings.Split(relative, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			http.NotFound(w, r)
			return
		}
		drive, ok := drives[parts[0]]
		if !ok {
			http.NotFound(w, r)
			return
		}
		serveStorageRESTOperation(w, r, drive, parts[1])
	})
}

func serveStorageRESTOperation(w http.ResponseWriter, r *http.Request, drive *storage.Disk, operation string) {
	bucket, object := r.URL.Query().Get("bucket"), r.URL.Query().Get("object")
	switch operation {
	case "bucket":
		if r.Method == http.MethodPut {
			writeStorageRESTError(w, drive.MakeBucket(bucket))
			return
		}
		if r.Method == http.MethodDelete {
			writeStorageRESTError(w, drive.DeleteBucket(bucket))
			return
		}
		if r.Method == http.MethodGet {
			info, err := drive.StatBucket(bucket)
			if err != nil {
				writeStorageRESTError(w, err)
				return
			}
			writeStorageRESTJSON(w, storageRESTFileInfo{Filename: info.Name(), Modified: info.ModTime()})
			return
		}
	case "buckets":
		if r.Method == http.MethodGet {
			infos, err := drive.ListBuckets()
			if err != nil {
				writeStorageRESTError(w, err)
				return
			}
			result := make([]storageRESTFileInfo, len(infos))
			for i, info := range infos {
				result[i] = storageRESTFileInfo{Filename: info.Name(), Modified: info.ModTime()}
			}
			writeStorageRESTJSON(w, result)
			return
		}
	case "shard":
		part, err := storageRESTPartNumber(r)
		if err != nil {
			writeStorageRESTError(w, err)
			return
		}
		dataDir := r.URL.Query().Get("data-dir")
		if r.Method == http.MethodPost {
			writer, err := drive.CreateShardFile(r.Context(), bucket, object, dataDir, part)
			if err != nil {
				writeStorageRESTError(w, err)
				return
			}
			_, err = io.Copy(writer, r.Body)
			err = errors.Join(err, writer.Close())
			writeStorageRESTError(w, err)
			return
		}
		if r.Method == http.MethodGet {
			offset, length, err := storageRESTRange(r)
			if err != nil {
				writeStorageRESTError(w, err)
				return
			}
			reader, err := drive.ReadShardFile(r.Context(), bucket, object, dataDir, part)
			if err != nil {
				writeStorageRESTError(w, err)
				return
			}
			buf := make([]byte, length)
			n, readErr := reader.ReadAt(buf, offset)
			closeErr := reader.Close()
			if readErr != nil && (!errors.Is(readErr, io.EOF) || n <= 0) {
				writeStorageRESTError(w, errors.Join(readErr, closeErr))
				return
			}
			if closeErr != nil {
				writeStorageRESTError(w, closeErr)
				return
			}
			writeStorageRESTBody(w, buf[:n])
			return
		}
	case "data":
		if r.Method == http.MethodDelete {
			writeStorageRESTError(w, drive.DeleteObjectData(bucket, object, r.URL.Query().Get("data-dir")))
			return
		}
	case "meta":
		if r.Method == http.MethodPut {
			data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err == nil {
				err = drive.WriteMetaTmp(bucket, object, data)
			}
			writeStorageRESTError(w, err)
			return
		}
		if r.Method == http.MethodGet {
			data, err := drive.ReadMeta(bucket, object)
			if err != nil {
				writeStorageRESTError(w, err)
				return
			}
			writeStorageRESTBody(w, data)
			return
		}
	case "rename-meta":
		if r.Method == http.MethodPost {
			writeStorageRESTError(w, drive.RenameMeta(bucket, object))
			return
		}
	case "upload-meta":
		uploadID, name := r.URL.Query().Get("upload-id"), r.URL.Query().Get("name")
		if uploadID == "" || name == "" || strings.ContainsAny(name, `/\\`) {
			writeStorageRESTError(w, errors.New("invalid upload metadata request"))
			return
		}
		if r.Method == http.MethodPut {
			data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err == nil {
				err = drive.WriteUploadMeta(bucket, object, uploadID, name, data)
			}
			writeStorageRESTError(w, err)
			return
		}
		if r.Method == http.MethodGet {
			data, err := drive.ReadUploadMeta(bucket, object, uploadID, name)
			if err != nil {
				writeStorageRESTError(w, err)
				return
			}
			writeStorageRESTBody(w, data)
			return
		}
	case "upload":
		if r.Method == http.MethodDelete {
			uploadID := r.URL.Query().Get("upload-id")
			if uploadID == "" {
				writeStorageRESTError(w, errors.New("invalid upload ID"))
				return
			}
			writeStorageRESTError(w, drive.DeleteUpload(bucket, object, uploadID))
			return
		}
	case "object":
		if r.Method == http.MethodDelete {
			writeStorageRESTError(w, drive.DeleteObject(bucket, object))
			return
		}
	case "objects":
		if r.Method == http.MethodGet {
			names, err := drive.ListObjects(bucket, r.URL.Query().Get("prefix"))
			if err != nil {
				writeStorageRESTError(w, err)
				return
			}
			writeStorageRESTJSON(w, names)
			return
		}
	}
	http.NotFound(w, r)
}

func storageRESTPartNumber(r *http.Request) (int, error) {
	part, err := strconv.Atoi(r.URL.Query().Get("part"))
	if err != nil || part < 1 {
		return 0, errors.New("invalid shard part")
	}
	return part, nil
}

func storageRESTRange(r *http.Request) (int64, int, error) {
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil || offset < 0 {
		return 0, 0, errors.New("invalid shard offset")
	}
	length, err := strconv.Atoi(r.URL.Query().Get("length"))
	if err != nil || length < 1 || length > 16<<20 {
		return 0, 0, errors.New("invalid shard length")
	}
	return offset, length, nil
}

func writeStorageRESTError(w http.ResponseWriter, err error) {
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if errors.Is(err, storage.ErrNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if errors.Is(err, storage.ErrBucketExists) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func writeStorageRESTJSON(w http.ResponseWriter, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		writeStorageRESTError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	writeStorageRESTBody(w, data)
}

func writeStorageRESTBody(w http.ResponseWriter, data []byte) {
	if _, err := w.Write(data); err != nil {
		log.Error().Err(err).Msg("failed to write storage response")
	}
}
