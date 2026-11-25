
```bash
./bin/start-uc-server 7071
./bin/uc
./bin/uc list
./bin/uc catalog list
./bin/uc 7071 catalog list
./bin/uc --server http://localhost:7071 catalog list
./bin/uc --server http://localhost:7071 catalog list
./bin/uc --server http://localhost:7071 catalog create --name mycatalog
./bin/uc --server http://localhost:7071 catalog list
./bin/uc --server http://localhost:7071 schema list
./bin/uc --server http://localhost:7071 --catalog mycatalog schema list
./bin/uc --server http://localhost:7071 --catalog mycatalog schema create --name schema1
./bin/uc --server http://localhost:7071 --catalog mycatalog schema list
./bin/uc --server http://localhost:7071 volume list
./bin/uc --server http://localhost:7071 volume --catalog mycatalog --schema schema1
./bin/uc --server http://localhost:7071  --catalog mycatalog --schema schema1 volume list
./bin/uc --server http://localhost:7071  --catalog mycatalog --schema schema1 volume create argus1
./bin/uc --server http://localhost:7071  --catalog mycatalog --schema schema1 volume create--full_name argus1
./bin/uc --server http://localhost:7071  --catalog mycatalog --schema schema1 volume create --full_name argus1
./bin/uc --server http://localhost:7071  --catalog mycatalog --schema schema1 volume create --full_name argus1 --storage_location test
./bin/uc --server http://localhost:7071  --catalog mycatalog --schema schema1 volume create --full_name argus1 --storage_location /opt/test
```