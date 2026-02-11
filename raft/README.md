Простая реализация алгоритма выбора лидера Raft


Start Nodes:
```
go run node.go 0 8080 http://localhost:8080,http://localhost:8081,http://localhost:8082,http://localhost:8083
go run node.go 1 8081 http://localhost:8080,http://localhost:8081,http://localhost:8082,http://localhost:8083
go run node.go 2 8082 http://localhost:8080,http://localhost:8081,http://localhost:8082,http://localhost:8083
go run node.go 3 8083 http://localhost:8080,http://localhost:8081,http://localhost:8082,http://localhost:8083
```

Client:
```
go build client.go
PUT: ./client PUT http://localhost:8080/data/1 '{"value":"11"}'
PUT PUT: ./client PUT http://localhost:8080/data/ '{"2":"22","3":"33"}'
GET DATA: ./client GET http://localhost:8080/data/1
GET LOG: ./client GET http://localhost:8080/log
DELETE: ./client DELETE http://localhost:8080/data/1
CAS: ./client CAS http://localhost:8080/data/1 '{"expect":"11", "update":"111"}'
WHOIS: ./client WHOIS http://localhost:8080
```
