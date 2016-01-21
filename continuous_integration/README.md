# Continious Integration

## Local test

Requirements:
-  `docker`
- `docker-compose`

Build the container:
```bash
docker build -t knit .
```

Start the container and wait for the it to be ready:

```bash
docker-compose up
```

To start a bash session in the running container:

```bash
# Get the container ID
export CONTAINER_ID=$(docker ps -l -q)

# Start the bash session
docker exec -it $CONTAINER_ID /bin/bash
```


```bash
hadoop jar ./knit-1.0-SNAPSHOT.jar io.continuum.knit.Client hdfs://localhost:9000/jars/knit-1.0-SNAPSHOT.jar 1 "python -c 'import sys; print(sys.path); import random; print(str(random.random()))'"
```

