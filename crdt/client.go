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
)

func main() {
    addr := flag.String("addr", "http://localhost:8081", "address")
    patch := flag.String("patch", "", "key=val,key=, (пустое - удалить)")
    dump := flag.Bool("dump", false, "вывести данные")
    vclock := flag.Bool("vclock", false, "вывести часы")
    flag.Parse()
    if *patch != "" {
        c := make(map[string]*string)
        for _, pair := range strings.Split(*patch, ",") {
            kv := strings.SplitN(pair, "=", 2)
            k := kv[0]
            var v *string
            if len(kv) == 2 && kv[1] != "" {
                vv := kv[1]
                v = &vv
            }
            c[k] = v
        }
        d, _ := json.Marshal(c)
        req, _ := http.NewRequest("PATCH", *addr+"/patch", bytes.NewReader(d))
        req.Header.Set("Content-Type", "application/json")
        res, err := http.DefaultClient.Do(req)
        if err != nil {
            fmt.Println("err:", err)
            os.Exit(1)
        }
        io.Copy(os.Stdout, res.Body)
        res.Body.Close()
        fmt.Println()
        return
    }
    if *dump {
        res, err := http.Get(*addr + "/dump")
        if err != nil {
            fmt.Println("err:", err)
            os.Exit(1)
        }
        io.Copy(os.Stdout, res.Body)
        res.Body.Close()
        fmt.Println()
        return
    }
    if *vclock {
        res, err := http.Get(*addr + "/vclock")
        if err != nil {
            fmt.Println("err:", err)
            os.Exit(1)
        }
        io.Copy(os.Stdout, res.Body)
        res.Body.Close()
        fmt.Println()
        return
    }
    fmt.Println("укажи -patch foo=bar,etc, -dump или -vclock")
}
