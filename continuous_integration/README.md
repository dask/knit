# Continious Integration

## Local test

Requirements:
-  `docker`
- `docker-compose`

Build the container:
```bash
docker build -t rambling .
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
docker exec -it $CONTAINER_ID bash
```


```bash
hadoop jar ./rambling-1.0-SNAPSHOT.jar com.continuumio.rambling.Client hdfs://localhost:8020/jars/rambling-1.0-SNAPSHOT.jar 1 "python -c 'import sys; print(sys.path); import random; print(str(random.random()))'"
```

