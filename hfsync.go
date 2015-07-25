package main

import (
    "crypto/sha1"
    "encoding/csv"
    "errors"
    "fmt"
    "io"
    "log"
    "net"
    "net/http"
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
}

var _G CONFIG

func init() {
    _G.WORKER_NUM = 3
    _G.UID = getUid()
    _G.HttpClient = &http.Client{}
    confFile := "hfsync.ini"

    // creating config
    _, err := os.Stat(confFile)
    if os.IsNotExist(err) {
        _G.Config.Server.Url = "http://some.server"
        _G.Config.Server.Port = 80
        _G.Config.Server.MaxSpeed = 1000
        _G.Config.Files.CheckTime = 21600
        _G.Config.Files.IgnoreList = []string{"some/files", "to/ignore"}

        file, err := os.Create(confFile)
        if err != nil { log.Fatal("Error creating config file.") }

        err = toml.NewEncoder(file).Encode(_G.Config)
        if err != nil { log.Fatalf("Error encoding TOML: %s", err) }
        log.Fatalf("Config file %s created, please edit it.", confFile)
    }

    // reading config
    _, err = toml.DecodeFile(confFile, &_G.Config)
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
    linkChan := make(chan string)
    wg := new(sync.WaitGroup)

    fmt.Printf("Username: %s\n", _G.Config.User.Name)
    fmt.Printf("Password: %s\n", _G.UID)
    fmt.Printf("Destination Folder: %s\n", _G.Config.Files.DlFolder)
    fmt.Printf("Ignore: %v\n", _G.Config.Files.IgnoreList)

    // creating workers
    for i := 0; i < _G.WORKER_NUM; i++ {
        wg.Add(1)
        go downloader(linkChan, wg)
    }

    for {
        // download file list
        csvpath, err := download("files.csv")
        if err != nil { log.Fatal(err) }

        // read csv
        csvfile, err := os.Open(csvpath)
        if err != nil { log.Fatal(err) }
        defer csvfile.Close()

        // process csv
        reader := csv.NewReader(csvfile)
        reader.FieldsPerRecord = -1

        rawCSVdata, err := reader.ReadAll()
        if err != nil { log.Fatal(err) }

        // for each file
        for _, line := range rawCSVdata {
            if (inIgnoreList(line[0])) {
                continue
            }

            srv_mod_date, err := strconv.ParseFloat(line[1], 32)
            stat, err := os.Stat(path.Join(_G.Config.Files.DlFolder, line[0]))

            if err != nil && !os.IsNotExist(err) {
                log.Println(err)

            // download if server file more recent or no local file
            } else if os.IsNotExist(err) || stat.ModTime().Unix() < int64(srv_mod_date) || stat.Size() == 0 {
                linkChan <- line[0]
            }
        }

        // wait for workers to finish
        close(linkChan)
        wg.Wait()

        // wait until next fetch
        time.Sleep(time.Duration(_G.Config.Files.CheckTime) * time.Second)
    }
}

func downloader(linkChan chan string, wg *sync.WaitGroup) {
    defer wg.Done()

    for url := range linkChan {
        download(url)
    }
}

func download(file_url string) (string, error) {
    file_path := path.Join(_G.Config.Files.DlFolder, file_url)
    full_url := _G.Config.Server.Url + ":" + strconv.Itoa(_G.Config.Server.Port) + "/" + file_url

    fmt.Printf("DOWNLOADING: %s\n         TO: %s\n", full_url, file_path)
    os.MkdirAll(path.Dir(file_path), os.FileMode(0755))

    file, err := os.Create(file_path + "__TMP")
    if err != nil { return "", err }
    defer file.Close()

    req, err := http.NewRequest("GET", full_url, nil)
    req.SetBasicAuth(_G.Config.User.Name, _G.UID)
    res, err := _G.HttpClient.Do(req)

    if err != nil { return "", err }
    if res.StatusCode != 200 { return "", errors.New(res.Status) }
    defer res.Body.Close()

    // handling speed limits
    for range time.Tick(1 * time.Second) {
        _, err := io.CopyN(file, res.Body, int64(_G.Config.Server.MaxSpeed))
        if err != nil && err.Error() == "EOF" {
            break
        } else if err != nil {
            return "", err
        }
    }
    os.Rename(file_path + "__TMP", file_path)

    return file_path, nil
}
