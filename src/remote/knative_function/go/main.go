package main

import (
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}

type GGPaths struct {
	blobs      string
	reductions string
}

func NewGGPaths() *GGPaths {
	curDir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	os.Setenv("PATH", fmt.Sprintf("%s:%s", curDir, os.Getenv("PATH")))

	if os.Getenv("GG_DIR") == "" {
		os.Setenv("GG_DIR", "/tmp/_gg")
	}

	if os.Getenv("GG_CACHE_DIR") == "" {
		os.Setenv("GG_CACHE_DIR", "/tmp/_gg/_cache")
	}

	os.MkdirAll("/tmp/_gg/_cache", 0777)
	os.MkdirAll("/tmp/_gg/blobs", 0777)
	os.MkdirAll("/tmp/_gg/reductions", 0777)

	ggDir := "/tmp/_gg"

	return &GGPaths{
		blobs:      filepath.Join(ggDir, "blobs"),
		reductions: filepath.Join(ggDir, "reductions"),
	}
}

func (gp *GGPaths) GetBlobPath(hash string) string {
	return filepath.Join(gp.blobs, hash)
}

func (gp *GGPaths) GetReductionPath(hash string) string {
	return filepath.Join(gp.reductions, hash)
}

const serverName = "serverless"

var ggPaths = NewGGPaths()
var curDir = ""

type Thunk struct {
	Data    string   `json:"data"`
	Hash    string   `json:"hash"`
	Outputs []string `json:"outputs"`
}

type RequestPayload struct {
	StorageBackend string  `json:"storageBackend"`
	Thunks         []Thunk `json:"thunks"`
	Timelog        bool    `json:"timelog"`
}

type ReturnOutputs struct {
	Tag        string `json:"tag"`
	Hash       string `json:"hash"`
	Size       int64  `json:"size"`
	Executable bool   `json:"executable"`
	Data       string `json:"data"`
}

type ExecutedThunk struct {
	ThunkHash string          `json:"thunkHash"`
	Outputs   []ReturnOutputs `json:"outputs"`
}

type Result struct {
	ReturnCode     int             `json:"returnCode"`
	Stdout         string          `json:"stdout"`
	ExecutedThunks []ExecutedThunk `json:"executedThunks,omitempty"`
}

func WriteResult(w http.ResponseWriter, result Result) {
	b, _ := json.Marshal(result)
	s := string(b)

	fmt.Fprint(w, s)
}

func MakeExecutable(path string) {
	err := os.Chmod(path, 0777)
	if err != nil {
		log.Fatal(err)
	}
}

func GGCacheCheck(hash, outputTag string) string {
	key := hash
	if len(outputTag) > 0 {
		key += fmt.Sprintf("#%s", outputTag)
	}

	rpath := ggPaths.GetReductionPath(key)
	b, err := os.ReadFile(rpath)
	if err != nil {
		/* if !os.IsExist(err) {
			log.Fatalf("Failed to read file in GGCacheCheck: %v", err)
		} */

		return ""
	}

	str := string(b)
	return strings.Split(str, " ")[0]
}

func IsHashForThunk(hash string) bool {
	return len(hash) > 0 && hash[0] == 'T'
}

func IsExecutable(path string) bool {
	fi, _ := os.Stat(path)
	return (fi.Mode() & 0100) != 0
}

func handler(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s: received a request", serverName)

	var reqPayload RequestPayload
	err := json.NewDecoder(r.Body).Decode(&reqPayload)
	if err != nil {
		log.Fatalf("Error reading body: %v", err)
		return
	}

	os.Setenv("GG_STORAGE_URI", reqPayload.StorageBackend)
	thunks := reqPayload.Thunks
	timelog := reqPayload.Timelog

	// Write thunks to disk
	for _, thunk := range thunks {
		thunkData, err := b64.StdEncoding.DecodeString(thunk.Data)
		if err != nil {
			log.Fatalf("Failed to decode thunk %v", err)
			return
		}

		blobPath := ggPaths.GetBlobPath(thunk.Hash)
		err = os.WriteFile(blobPath, thunkData, 0777)
		if err != nil {
			log.Fatalf("Error write thunk data: %v", err)
			return
		}
	}

	// Move executables from Lambda package to .gg directory
	executableDir := filepath.Join(curDir, "executables")
	if _, err := os.Stat(executableDir); err == nil || os.IsExist(err) {
		files, err := ioutil.ReadDir(executableDir)
		if err != nil {
			log.Fatalf("Error read executable directory: %v", err)
			return
		}

		for _, f := range files {
			name := f.Name()
			blobPah := ggPaths.GetBlobPath(name)
			exePath := filepath.Join(executableDir, name)

			if _, err := os.Stat(blobPah); err != nil && os.IsExist(err) {
				err = copyFileContents(exePath, blobPah)
				if err != nil {
					log.Fatalf("Error copy file: %v", err)
					return
				}

				MakeExecutable(blobPah)
			}
		}
	}

	// Remove old thunk-execute directories
	cmd := exec.Command("rm", "-rf", "/tmp/thunk-execute.*")
	_, err = cmd.Output()
	if err != nil {
		log.Fatalf("Error Remove old thunk-execute directories: %v", err)
		// http.Error(w, "can't Remove old thunk-execute directories", http.StatusBadRequest)
		return
	}

	// Execute the thunk, and upload the result
	command := []string{
		"--get-dependencies",
		"--put-output",
		"--cleanup",
	}

	if timelog {
		command = append(command, "--timelog")
	}

	for _, thunk := range thunks {
		command = append(command, thunk.Hash)
	}

	fmt.Println()

	returnCode := 0
	stdOut := ""

	for _, arg := range command {
		fmt.Printf("%s ", arg)
	}

	cmd = exec.Command("./gg-execute-static", command...)
	outputByte, err := cmd.Output()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			returnCode = exitError.ExitCode()
		}
	}

	stdOut = string(outputByte)

	executedThunks := []ExecutedThunk{}
	for _, thunk := range thunks {
		outputs := []ReturnOutputs{}

		for _, outputTag := range thunk.Outputs {
			outputHash := GGCacheCheck(thunk.Hash, outputTag)

			if len(outputHash) == 0 {
				WriteResult(w, Result{
					Stdout:     stdOut,
					ReturnCode: returnCode,
				})

				return
			}

			data := ""
			blobPath := ggPaths.GetBlobPath(outputHash)
			if IsHashForThunk(outputHash) {
				b, err := os.ReadFile(blobPath)
				if err != nil {
					log.Fatalf("Error read file: %v", err)
				}

				data = b64.StdEncoding.EncodeToString(b)
			}

			fi, err := os.Stat(blobPath)
			if err != nil {
				log.Fatalf("Error get size: %v", err)
			}

			outputs = append(outputs, ReturnOutputs{
				Tag:        outputTag,
				Hash:       outputHash,
				Size:       fi.Size(),
				Executable: IsExecutable(blobPath),
				Data:       data,
			})
		}

		executedThunks = append(executedThunks, ExecutedThunk{
			ThunkHash: thunk.Hash,
			Outputs:   outputs,
		})
	}

	WriteResult(w, Result{
		ReturnCode:     0,
		Stdout:         "",
		ExecutedThunks: executedThunks,
	})
}

func main() {
	log.Printf("%s: starting server...", serverName)

	http.HandleFunc("/", handler)
	http.HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, "pong")
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "80"
	}

	log.Printf("%s: listening on port %s", serverName, port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
