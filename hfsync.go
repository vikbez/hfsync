package main

import (
    "crypto/sha1"
    "encoding/csv"
    "errors"
    "flag"
    "fmt"
    "io"
    "log"
    "net"
    "net/http"
    "net/url"
    "os"
    "path"
    "strconv"
    "strings"
    "sync"
    "time"
    "github.com/BurntSushi/toml"
)

type tomlConfig struct {
    User        tomlUser    `toml:"user"`
    Server      tomlServer  `toml:"server"`
    Files       tomlFiles   `toml:"files"`
}

type tomlUser struct {
    Name        string      `toml:"name"`
}

type tomlServer struct {
    Url         string      `toml:"url"`
    Port        int         `toml:"port"`
    MaxSpeed    int         `toml:"download_speed"`
}

type tomlFiles struct {
    CheckTime   int         `toml:"check_time"`
    IgnoreList  []string    `toml:"ignore_list"`
    DlFolder    string      `toml:"download_folder"`
}

type CONFIG struct {
    Config      tomlConfig
    WORKER_NUM  int
    UID         string
    HttpClient  *http.Client
    wg          *sync.WaitGroup
}

var _G CONFIG

func init() {
    _G.WORKER_NUM = 3
    _G.UID = getUid()
    _G.HttpClient = &http.Client{}
    _G.wg = new(sync.WaitGroup)

    var keyFlag = flag.Bool("key", false, "output the private key then exit")
    var confFile = flag.String("config", "hfsync.ini", "specify config file path")
    flag.Parse()
    if *keyFlag {
        fmt.Println(getUid())
        os.Exit(0)
    }

    // creating config
    _, err := os.Stat(*confFile)
    if os.IsNotExist(err) {
        _G.Config.Server.Url = "http://some.server"
        _G.Config.Server.Port = 80
        _G.Config.Server.MaxSpeed = 1000
        _G.Config.Files.CheckTime = 21600
        _G.Config.Files.IgnoreList = []string{"some/files", "to/ignore"}

        file, err := os.Create(*confFile)
        if err != nil { log.Fatal("Error creating config file.") }

        err = toml.NewEncoder(file).Encode(_G.Config)
        if err != nil { log.Fatalf("Error encoding TOML: %s", err) }
        log.Fatalf("Config file %s created, please edit it.", *confFile)
    }

    // reading config
    _, err = toml.DecodeFile(*confFile, &_G.Config)
    if err != nil { log.Fatalf("Error decoding config file: %s", err) }

    if _G.Config.Files.DlFolder == "" {
        log.Fatal(errors.New("Download Folder not defined, please edit config file"))
    }

    // this is not exact science
    _G.Config.Server.MaxSpeed = _G.Config.Server.MaxSpeed * 1000 / _G.WORKER_NUM
}

// Unique id generation
func getUid() (string) {
    uidStr, err := os.Hostname()
    Interfaces, err := net.Interfaces()
    if err != nil { log.Fatal(err) }

    for _, inter := range Interfaces {
        uidStr += inter.HardwareAddr.String()
    }

    sha1uid := fmt.Sprintf("%x", sha1.Sum([]byte(uidStr)))
    return sha1uid
}

func inIgnoreList(url string) (bool) {
    for _, val := range _G.Config.Files.IgnoreList {
        if strings.HasPrefix(url, val) {
            return true
        }
    }
    return false
}

func main() {
    fmt.Printf("Destination Folder:\n    %s\n", _G.Config.Files.DlFolder)
    fmt.Println("Ignore list:")
    for _, val := range _G.Config.Files.IgnoreList {
        fmt.Printf("    %s\n", val)
    }
    var totalSize int64
    var doneSize int64
    var size int64

    var todo []*[]string

    for {
        linkChan := make(chan string)
        // creating workers
        for i := 0; i < _G.WORKER_NUM; i++ {
            _G.wg.Add(1)
            go downloader(linkChan)
        }

        // download file list
        fmt.Println("Downloading file index ...")
        csvpath, err := download("files.csv")
        if err != nil { log.Fatal(err) }

        // read csv
        csvfile, err := os.Open(csvpath)
        if err != nil { log.Fatal(err) }

        // process csv
        reader := csv.NewReader(csvfile)
        reader.FieldsPerRecord = -1

        rawCSVdata, err := reader.ReadAll()
        if err != nil { log.Fatal(err) }
        err = csvfile.Close()
        if err != nil { log.Fatal(err) }

        totalSize = 0
        // for each file
        for i, line := range rawCSVdata {
            if (inIgnoreList(line[0])) { continue }

            srv_mod_date, err := strconv.ParseFloat(line[1], 32)
            stat, err := os.Stat(path.Join(_G.Config.Files.DlFolder, line[0]))

            // file stat error other than file does not exist
            if err != nil && !os.IsNotExist(err) {
                log.Println(err)

            // download if server file more recent or no local file
            } else if os.IsNotExist(err) || stat.ModTime().Unix() < int64(srv_mod_date) {
                size, _ = strconv.ParseInt(line[2], 10, 64)
                totalSize += size
                todo = append(todo, &rawCSVdata[i])
            }
        }

        doneSize = 0
        // actually download the files
        for i := range todo {
            fmt.Printf("[%d%%] - %s\n", (100 * doneSize / totalSize), (*todo[i])[0])
            size, _ = strconv.ParseInt((*todo[i])[2], 10, 64)
            doneSize += size
            linkChan <- (*todo[i])[0]
        }

        todo = nil
        // wait for workers to finish
        close(linkChan)
        _G.wg.Wait()

        // quit if wait time < 1
        if (_G.Config.Files.CheckTime < 1) { break; }
        // wait until next fetch
        fmt.Println("Waiting.")
        time.Sleep(time.Duration(_G.Config.Files.CheckTime) * time.Second)
    }
}

func downloader(linkChan chan string) {
    defer _G.wg.Done()
    for url := range linkChan {
        _, err := download(url)
        if err != nil { log.Println(err) }
    }
}

func download(file_url string) (string, error) {
    file_path := path.Join(_G.Config.Files.DlFolder, file_url)
    file_url = strings.Replace(url.QueryEscape(file_url), "%2F", "/", -1)
    file_url = strings.Replace(file_url, "+", "%20", -1)
    full_url := _G.Config.Server.Url + ":" + strconv.Itoa(_G.Config.Server.Port) + "/" + file_url

    os.MkdirAll(path.Dir(file_path), os.FileMode(0755))

    file, err := os.Create(file_path + "_TMP")
    if err != nil { return "", err }
    defer file.Close()

    req, err := http.NewRequest("GET", full_url, nil)
    req.SetBasicAuth(_G.UID, _G.Config.User.Name)
    res, err := _G.HttpClient.Do(req)
    if err != nil { return "", err }
    defer res.Body.Close()
    if res.StatusCode != 200 { return "", errors.New(res.Status) }

    // handling speed limits
    for range time.Tick(1 * time.Second) {
        _, err := io.CopyN(file, res.Body, int64(_G.Config.Server.MaxSpeed))
        if err != nil && err.Error() == "EOF" {
            break
        } else if err != nil {
            return "", err
        }
    }
    err = file.Close()
    if err != nil { return "", err }

    os.Remove(file_path)
    err = os.Rename(file_path + "_TMP", file_path)
    if err != nil { return "", err }

    return file_path, nil
}
