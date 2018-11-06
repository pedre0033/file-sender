package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/vladimirvivien/gowfs"
	yaml "gopkg.in/yaml.v2"
)

// Conf ...
type Conf struct {
	ToBeProcessedPath string `yaml:"tobeprocessedpath"`
	HdfsEndpointIP    string `yaml:"hdfsip,omitempty"`
	HdfsEndpointPort  string `yaml:"hdfsport,omitempty"`
	ProcessedPath     string `yaml:"processedpath"`
	DstPath           string `yaml:"dstpath,omitempty"`
}

// FileStats ...
type FileStats struct {
	Path     string
	FileName string
}

func main() {
	fshdfs, err := gowfs.NewFileSystem(gowfs.Configuration{Addr: "localhost:50070", User: "hdfs"})
	if err != nil {
		log.Fatal(err)
	}
	var confVars Conf
	var wg sync.WaitGroup
	filesInfoChan := make(chan FileStats)
	configfilepath := flag.String("c", "/opt/file-sender/config.yaml", "Configuration file path")
	flag.Parse()

	yamlFile, err := ioutil.ReadFile(*configfilepath)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, &confVars)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	wg.Add(1)
	go filestobeproc(confVars.ToBeProcessedPath, filesInfoChan, &wg)
	wg.Add(1)
	go processiofiles(filesInfoChan, confVars.ProcessedPath, &wg, fshdfs)
	wg.Wait()

}

func filestobeproc(rootpath string, ch chan FileStats, wg *sync.WaitGroup) chan FileStats {
	defer wg.Done()
	defer close(ch)
	filepath.Walk(rootpath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			ch <- FileStats{Path: path, FileName: info.Name()}
		}
		return nil
	})
	return ch
}

func processiofiles(ch <-chan FileStats, procpath string, wg *sync.WaitGroup, fshdfs *gowfs.FileSystem) error {
	if _, err := os.Stat(procpath); os.IsNotExist(err) {
		os.Mkdir(procpath, os.FileMode(uint32(0777)))
	}
	defer wg.Done()
	buildString := bytes.Buffer{}
	var err error
	for fs := range ch {
		buildString.WriteString(procpath)
		buildString.WriteString("/")
		buildString.WriteString(fs.FileName)
		oldpath, _ := os.Open(fs.Path)
		ok, er := fshdfs.Create(
			oldpath,
			gowfs.Path{Name: "/tmp/" + fs.FileName},
			false,
			0,
			0,
			0700,
			0,
		)
		if ok {
			err = os.Rename(fs.Path, buildString.String())
			buildString.Reset()
		} else {
			panic(er)
		}

	}
	if err != nil {
		panic(err)
	}
	return nil
}
