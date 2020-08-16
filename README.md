# sjydb
...  
SJYDB is a lightweight database compatible with reids protocol based on rocksdb storage engine. It now supports most frequently-used redis commands such as set key value, You can view all the commands on Redis' official website:  https://redis.io/commands  


  
    
to use your local machine as server:         (change ip in server.go to your own ip)                                  
go run server.go read.go write.go opendb.go catch.go funcs.go  
to open the client to link a server:  
go run client.go funcs.go  
change IP in client.go to choose the server you want to link. Default is mine  
