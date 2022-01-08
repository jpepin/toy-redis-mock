
# spin up a redis container to get the redis cli, and point at localhost
redis-cli-localhost:
	docker run -it --network="host" --rm redis redis-cli -h 127.0.0.1
