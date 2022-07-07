Project 6: Distributed Store
============================

In this project, you will build the infrastructure for a small social network. In particular, you will work on a distributed, sharded key-value store that stores the social graph and user information for the social network.

# How to Run:

Run `./install.sh` to install gRPC and its dependencies.

To build your code, run `make` from inside the build directory.

To test you code, run `./test.sh` or `make check` inside the build directory.

## Running the frontend

Inside the course container, after making all of the executables, start up a shardmaster on port 9095, shardkv servers, and a client in separate terminals.

```
./shardmaster 9095

./client <SHARDMASTER_HOST> 9095

./shardkv <PORT> <SHARDMASTER_HOST> 9095
```

Start as many shardkv servers as you would like and add them using the client's `join` command (e.g. `join <SHARDMASTER_HOST>:<PORT>`). You can verify that they've been added using the client's `query` command.
The shardmaster host name will be printed after starting up the shardmaster -- this is should be the ID of the cs300 docker container.

Outside of the course container, navigate to this directory and run `docker compose up`. The Bacefook UI should be now viewable at http://localhost:3000 in your browser!

Note: In order for the frontend to work correctly, you must enter `user_<NUM>` and `post_<NUM>` in the user/post ID fields where NUM is between 0 and 1000.

## Design Overview:
This project aims to build a distributed, sharded key-value storage system that stores user data and post data. It implements a naive load balancing strategy that involves a coordinator assigning shards of the key space to each server in the cluster given a fixed key space while allowing servers to join and leave. The bottle neck that disallows fault tolerance of this strategy is the failure of the coordinator and the failure of server, as no replication of the failed node's state can be transferred to other nodes in case of sudden failure. Those are beyond the scope of this project.

## Collaborators:
None

## Conceptual Questions:
1. The coordinator (shardmaster/controller), the followers (key-value servers), and the frontend clients together constitute the distributed system of BaceFook. In the process, when the web user sends an HTTP request to GET/PUT/UPDATE/DELETE a key, the frontend server (our Client, in the distributed system), after acquiring server_shard mapping, issues a RPC to the server responsible for the key. Internally, each server is responsible for a shard of keys assigned by the coordinator, which mostly handles load balancing across all the servers in the cluster as they join or leave through a n-way partition. The follower would return error if it's not in charge of the key it's responsible for. The client side (which we won't be implementing in the project) upon receiving the status (ok/err) could re-issue RPC call on other servers after connsulting the coordinator. An effective approach for the client to get an ok status faster could be actively sending RPC request to all followers, or another scheme could involve each follower give a hint for the client which other server it should contact next (which we are not implementing in this project). Internally the followers send heartbeat messages on a set interval to the coordinator to query the keys it's responsible for and update it's state internally, which could include issuing PUT RPC to transfer keys (retry until success) it's no longer responsible for. Beyond sending heartbeat messages, it processes RPC calls from client and handles the key values accordingly.

2. In the case of shardmaster handling all requests from client and assign jobs to each follower would make the shardmaster the bottleneck and decrease system throughput. This way we put all the load to the shardmaster and can't really achieve the distributed nature of the project. Upon a failed shardmaster, the system would be down completely as it won't be able to assign work to any of the servers. By having the servers take jobs, we avoid the single point of failure to some extent. If we have multiple clients issuing requests, each of the server would be taking jobs directly from the client depending on the responsible key space, which increase the system throughput and increase fault tolerance. 

## Extra Credit Attempted:
I completed the GDPR delete part of the extra credit in code and edit the makefile to make it compile correctly. (see GDPR delete)

Idea for implementing dynamic sharding if I have more time for this project: 
A mechanism for dynamic sharding is using consistent hashing. This involves using dividing the key space to shards using hash function with modulo on both the key and the server id: say we still have the key space N = 1000, we assign a position for the server in this key space by hash(server_id)%N, then we assign value for the client key by hash(key_id)%N, each server is responsible for the values after it's position and before the next server's position on the ring (the last server wraps around to cover for hashes before the first server's position). This way we can achieve a consistent hashing mechanism moves minimum amount of client keys upon server joining and leaving. 

## How long did it take to complete Distributed Store?
Somewhere between 15-20 hours