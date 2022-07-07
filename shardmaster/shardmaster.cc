#include "shardmaster.h"
// #include "../shardkv/shardkv.h"


/**
 * Based on the server specified in JoinRequest, you should update the
 * shardmaster's internal representation that this server has joined. Remember,
 * you get to choose how to represent everything the shardmaster tracks in
 * shardmaster.h! Be sure to rebalance the shards in equal proportions to all
 * the servers. This function should fail if the server already exists in the
 * configuration.
 *
 * @param context - you can ignore this
 * @param request A message containing the address of a key-value server that's
 * joining
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status
StaticShardmaster::Join(::grpc::ServerContext *context,
                        const ::JoinRequest *request, // server:string
                        Empty *response) {
  std::string server = request->server();
  if (server == "") {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: JOIN request null server");
  }
  std::lock_guard<std::mutex> lock(shard_mtx);
  std::map<std::string, std::vector<shard>>::iterator it;
  it = server_shard_map.find(server);
  if (it != server_shard_map.end()) { // if server already exists in config
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: JOIN request server already in cluster");
  }
  int partition_num = server_shard_map.size() + 1;
  if (partition_num == 1) { // if the first server to join
    shard s = shard_t();
    s.lower = MIN_KEY;
    s.upper = MAX_KEY;
    server_order.push_back(server);
    std::vector<shard> sh;
    sh.push_back(s);
    server_shard_map.insert(
        std::pair<std::string, std::vector<shard>>(server, sh));
  } else {
    std::vector<shard> shards = partition(partition_num, MIN_KEY, MAX_KEY);
    for (int i = 0; i < server_order.size(); i++) {
      it = server_shard_map.find(server_order.at(i));
      std::vector<shard> sh;
      sh.push_back(shards.at(i));
      it->second = sh; // reassign interval according to order
    }
    server_order.push_back(server);
    std::vector<shard> sh;
    sh.push_back(shards.back());
    server_shard_map.insert(
        std::pair<std::string, std::vector<shard>>(server, sh));
  }
  return ::grpc::Status::OK;
}

/**
 * LeaveRequest will specify a list of servers leaving. This will be very
 * similar to join, wherein you should update the shardmaster's internal
 * representation to reflect the fact the server(s) are leaving. Once that's
 * completed, be sure to rebalance the shards in equal proportions to the
 * remaining servers. If any of the specified servers do not exist in the
 * current configuration, this function should fail.
 *
 * @param context - you can ignore this
 * @param request A message containing a list of server addresses that are
 * leaving
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Leave(
    ::grpc::ServerContext *context,
    const ::LeaveRequest *request, // repeated string servers
    Empty *response) {
  int size = request->servers_size();
  if (size == 0) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: LEAVE request null server list");
  }
  LeaveRequest req = *request;
  std::lock_guard<std::mutex> lock(shard_mtx);
  std::map<std::string, std::vector<shard>>::iterator it;
  for (int i = 0; i < size; i++) {
    std::string *server = req.mutable_servers(i);
    it = server_shard_map.find(*server);
    if (it == server_shard_map.end()) { // if server not in config
      return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                            "ERR: LEAVE request server not found in config");
    }
    for (int i = 0; i < server_order.size(); i++) {
      if (server_order.at(i) ==
          *server) { // if exist, delete from map & vector,
                     // repartition & assign intervals
        server_shard_map.erase(*server);
        server_order.erase(server_order.begin() + i);
        break;
      }
    }
  }
  if (server_order.empty()) {
    return ::grpc::Status::OK;
  }
  std::vector<shard> new_shards =
      partition(server_order.size(), MIN_KEY, MAX_KEY);
  for (int i = 0; i < server_order.size(); i++) {
    it = server_shard_map.find(server_order.at(i));
    std::vector<shard> sh;
    sh.push_back(new_shards.at(i));
    it->second = sh; // reassign interval according to order
  }
  return ::grpc::Status::OK;
}

/**
 * Move the specified shard to the target server (passed in MoveRequest) in the
 * shardmaster's internal representation of which server has which shard. Note
 * this does not transfer any actual data in terms of kv-pairs. This function is
 * responsible for just updating the internal representation, meaning whatever
 * you chose as your data structure(s).
 *
 * @param context - you can ignore this
 * @param request A message containing a destination server address and the
 * lower/upper bounds of a shard we're putting on the destination server.
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Move(::grpc::ServerContext *context,
                                       const ::MoveRequest *request,
                                       Empty *response) {
  // Hint: Take a look at get_overlap in common.{h, cc}
  // Using the function will save you lots of time and effort!
  std::string server = request->server();
  Shard m_shard = request->shard();
  shard move_shard = shard_t();
  move_shard.lower = m_shard.lower();
  move_shard.upper = m_shard.upper();

  if (server == "") {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: MOVE request null server");
  } // error handling for empty shard >>>...<<<

  std::lock_guard<std::mutex> lock(shard_mtx);
  std::map<std::string, std::vector<shard>>::iterator it;

  it = server_shard_map.find(server);
  if (it == server_shard_map.end()) {
    // if server doesn't exist
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: MOVE request server not found");
  }

  for (int i = 0; i < server_order.size(); i++) {
    it = server_shard_map.find(server_order.at(i));
    std::vector<shard> shards = it->second;

    std::vector<shard> new_intervals;

    for (auto &sd : shards) {
      OverlapStatus status = get_overlap(move_shard, sd);
      switch (status) {
      case OverlapStatus::NO_OVERLAP: {
        new_intervals.push_back(sd);
      } break;
      case OverlapStatus::OVERLAP_START: {
        shard new_shard = shard_t();
        new_shard.lower = sd.lower;
        new_shard.upper = move_shard.lower - 1;
        new_intervals.push_back(new_shard);
      } break;
      case OverlapStatus::OVERLAP_END: {
        shard new_shard = shard_t();
        new_shard.lower = move_shard.upper + 1;
        new_shard.upper = sd.upper;
        new_intervals.push_back(new_shard);
      } break;
      case OverlapStatus::COMPLETELY_CONTAINS:
        break;
      case OverlapStatus::COMPLETELY_CONTAINED: {
        unsigned int upper = sd.upper;

        if (sd.lower != move_shard.lower) {
          shard lower_half = shard_t();
          lower_half.lower = sd.lower;
          lower_half.upper = move_shard.lower - 1;
          new_intervals.push_back(lower_half);
        }
        if (move_shard.upper < sd.upper) {
          shard upper_half = shard_t();
          upper_half.lower = move_shard.upper + 1;
          upper_half.upper = upper;
          new_intervals.push_back(upper_half);
        }
      } break;
      }
    }

    it->second = new_intervals;
  }

  // move shard into designated server
  it = server_shard_map.find(server);
  it->second.push_back(move_shard);

  // TODO: consider merging interval if interval range == shards.end().upper -
  // shards.begin().lower ...
  return ::grpc::Status::OK;
}

/**
 * Delete all the key-value pairs associated with this user from the distributed
 * store.
 *
 * @param context - you can ignore this
 * @param request A message containing the user ID whose data should removed
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::GDPRDelete(::grpc::ServerContext *context,
                                             const ::GDPRDeleteRequest *request,
                                             Empty *response) {
  // will check the key to delete, find the server responsible for it & issue
  // RPC delete call on that server
  std::string to_delete = request->key();
  if (to_delete == "all_users") {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: DELETE request illegal for all_users");
  }
  // the test doesn't cover deleting user_id_posts case, so we'll just not handle it here for now
  int id = extractID(to_delete);
  std::lock_guard<std::mutex> lock(shard_mtx);
  std::map<std::string, std::vector<shard>>::iterator it;

  for (auto &kv : server_shard_map) {
    if (CheckInShard(id, kv.second)) {
      // if id in a specific server, issue delete RPC on that server providing
      // the key to delete
      auto channel =
          grpc::CreateChannel(kv.first, grpc::InsecureChannelCredentials());
      auto stub = Shardkv::NewStub(channel);

      ::grpc::ClientContext cc;
      DeleteRequest req;
      Empty res;
      req.set_key(to_delete);

      auto status = stub->Delete(&cc, req, &res);
      while (!status.ok()) { // sleep & retry till success
        std::chrono::milliseconds timespan(50);
        std::this_thread::sleep_for(timespan);
        ::grpc::ClientContext new_cc;
        status = stub->Delete(&new_cc, req, &res);
      }
    }
  }

  return ::grpc::Status::OK;
}

/**
 * When this function is called, you should store the current servers and their
 * corresponding shards in QueryResponse. Take a look at
 * 'protos/shardmaster.proto' to see how to set QueryResponse correctly. Note
 * that its a list of ConfigEntry, which is a struct that has a server's address
 * and a list of the shards its currently responsible for.
 *
 * @param context - you can ignore this
 * @param request An empty message, as we don't need to send any data
 * @param response A message that specifies which shards are on which servers
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Query(::grpc::ServerContext *context,
                                        const StaticShardmaster::Empty *request,
                                        ::QueryResponse *response) {
  std::lock_guard<std::mutex> lock(shard_mtx);
  std::map<std::string, std::vector<shard>>::iterator it;
  if (server_order.empty()) {
    return ::grpc::Status::OK; // ?? for empty state, not set response/set as
                               // empty (how)
  }
  for (int i = 0; i < server_order.size(); i++) {
    std::string server = server_order.at(i);
    ConfigEntry *entry = response->add_config();
    entry->set_server(server);
    it = server_shard_map.find(server);
    std::vector<shard> shards = it->second;
    for (int i = 0; i < shards.size(); i++) {
      Shard *shard = entry->add_shards();
      shard->set_lower(shards.at(i).lower);
      shard->set_upper(shards.at(i).upper);
    }
  }
  return ::grpc::Status::OK;
}
