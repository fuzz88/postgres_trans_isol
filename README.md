```
#!/bin/bash
docker run --name temp-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 --rm -d postgres:latest
```