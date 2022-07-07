#include <grpcpp/grpcpp.h>

#include "shardkv.h"

/**
 * This method is analogous to a hashmap lookup. A key is supplied in the
 * request and if its value can be found, we should either set the appropriate
 * field in the response Otherwise, we should return an error. An error should
 * also be returned if the server is not responsible for the specified key
 *
 * @param context - you can ignore this
 * @param request a message containing a key
 * @param response we store the value for the specified key here
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Get(::grpc::ServerContext *context,
                                  const ::GetRequest *request,
                                  ::GetResponse *response) {

  std::string key = request->key();
  if (key == "") {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: GET request key null");
  }

  std::lock_guard<std::mutex> lock(kv_mutex);
  std::map<std::string, std::string>::iterator it;
  if (key == "all_users") {
    it = kv_store.find("all_users");
    std::string all_users = it->second;
    response->set_data(all_users);
    return ::grpc::Status::OK;
  }
  // for key of type user_id, post_id, and user_id_posts
  int id = extractID(key);

  // if current server not responsible for key
  if (CheckInShard(id, local_shard) == false) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: server not responsible for key");
  }

  it = kv_store.find(key);
  if (it == kv_store.end()) {
    // if not found
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "GET request key not found");
  }
  // on success, set data for rsp
  std::string value = it->second;
  response->set_data(value);
  return ::grpc::Status::OK;
}

/**
 * Insert the given key-value mapping into our store such that future gets will
 * retrieve it
 * If the item already exists, you must replace its previous value.
 * This function should error if the server is not responsible for the specified
 * key.
 *
 * @param context - you can ignore this
 * @param request A message containing a key-value pair
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Put(::grpc::ServerContext *context,
                                  const ::PutRequest *request,
                                  Empty *response) {
  std::string key = request->key();
  std::string data = request->data();
  std::string user = request->user();
  if (key == "") {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: PUT request key null");
  }
  // parse key, if key is of type user_id: check if user_id from key == user,
  // then check if key is in the shard,
  // then check if key already in map, if so, update value to data
  // if not, insert the kvpair, find the all_users in map, append user_id; also
  // append for the all_users list
  if (key == "all_users") {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: PUT request all users invalid");
  }

  // case of key == user_id, post_id, or user_id_posts
  std::vector<std::string> parsed = parse_value(key, "_");

  std::lock_guard<std::mutex> lock(kv_mutex);
  std::map<std::string, std::string>::iterator it;

  int uid = extractID(key);
  // if key not in local shard range (for user_id, post_id, and user_id_posts)
  if (CheckInShard(uid, local_shard) == false) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: PUT request server not responsible for key");
  }

  // special case: internal PUT where user field is "" & it's transfering a
  // "post"
  if (parsed[0] == "post" && parsed.size() == 2 && user == "") {
    it = kv_store.find(key);
    if (it == kv_store.end()) {
      kv_store.insert(std::pair<std::string, std::string>(key, data));
    } else {
      it->second = data;
    }
    return ::grpc::Status::OK;
  }

  // internal transfer for "user_id_posts": user field is ""
  if (parsed.size() == 3 && (parsed[0] == "user" && parsed[2] == "posts") &&
      user == "") {
    it = kv_store.find(key);
    if (it == kv_store.end()) {
      kv_store.insert(std::pair<std::string, std::string>(key, data));
    } else {
      it->second = data;
    }
    return ::grpc::Status::OK;
  }

  if (parsed[0] == "user" && parsed.size() == 2) { // key is of type "user_id"
    // if key not in kvstore
    it = kv_store.find(key);
    if (it == kv_store.end()) {
      // if not found, set user_id -> name (str)
      kv_store.insert(std::pair<std::string, std::string>(key, data));
      // add to all_users both in map & in list
      it = kv_store.find("all_users");
      it->second = it->second + key + ","; // cat new "user_id,"
    } else { // otherwise, this user already exist in local kvstore, just change
             // the value
      it->second = data;
    }
    return ::grpc::Status::OK;
  }
  // if key is of type post_id, check if key is in shard,
  // then check if key already in map, if so, update value to data
  // if not, insert the kvpair, check if user_id is in local shard range,
  // if so, check if user is new, if so, insert into
  // all_users, also update user_id_posts with post_id
  else if (parsed[0] == "post" && parsed.size() == 2 &&
           user != "") { // key is of type "post_id" & non empty user_id

    it = kv_store.find(key);
    if (it == kv_store.end()) { // if post_id not found
      // set post_id -> text (str)
      kv_store.insert(std::pair<std::string, std::string>(key, data));
      // check if user_id_posts/user_id is in local shard range
      int uuid = extractID(user);
      if (CheckInShard(uuid, local_shard) == false) {
        // APPEND request call in another server to append post_id to
        // user_id_posts
        for (auto &server : server_shard_map) {
          if (CheckInShard(uuid, server.second)) {
            auto channel = grpc::CreateChannel(
                server.first, grpc::InsecureChannelCredentials());
            auto stub = Shardkv::NewStub(channel);

            ::grpc::ClientContext cc;
            AppendRequest req;
            Empty res;
            req.set_key(user + "_posts");
            req.set_data(key + ",");

            auto status = stub->Append(&cc, req, &res);
            while (!status.ok()) { // sleep & retry till success
              std::chrono::milliseconds timespan(50);
              std::this_thread::sleep_for(timespan);
              ::grpc::ClientContext new_cc;
              status = stub->Append(&new_cc, req, &res);
            }
          }
        }
      } else {
        it = kv_store.find(user);
        if (it == kv_store.end()) {
          // if user is new (here we are sure user_id is also in shard range of
          // this
          // server), add to map with value "" --> in tests we shouldn't reach
          // this state
          kv_store.insert(std::pair<std::string, std::string>(user, ""));
          it = kv_store.find("all_users");
          it->second = it->second + user + ","; // cat new "user_id,"
        }
        // otherwise, user_id already exists
        // if user_id_post not already in local kv_store, create a mapping & add
        // the post, otherwise append new post to the user_id_posts
        it = kv_store.find(user + "_posts");
        if (it == kv_store.end()) {
          kv_store.insert(
              std::pair<std::string, std::string>(user + "_posts", key + ","));
        } else {
          it->second = it->second + key + ",";
        }
      }
    } else { // if post_id found, just update the data
      it->second = data;
    }
  }
  return ::grpc::Status::OK;
}

/**
 * Appends the data in the request to whatever data the specified key maps
 * to. If the key is not mapped to anything, this method should be
 * equivalent to a put for the specified key and value. If the server is not
 * responsible for the specified key, this function should fail.
 *
 * @param context - you can ignore this
 * @param request A message containngi a key-value pair
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>"
 */
::grpc::Status ShardkvServer::Append(::grpc::ServerContext *context,
                                     const ::AppendRequest *request,
                                     Empty *response) {
  // what can't we append to: all_users (should be illegal behavior),
  // user_id_posts (we don't have user field), otherwise for user_id and post_id
  // we can just append to data if appropriate
  std::string key = request->key();
  std::string data = request->data();
  if (key == "") {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: APPEND request key null");
  }
  if (key == "all_users") {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: APPEND request all users illegal behavior");
  }
  std::vector<std::string> parsed = parse_value(key, "_");
  std::lock_guard<std::mutex> lock(kv_mutex);
  std::map<std::string, std::string>::iterator it;

  int id = extractID(key);
  // check if id is in local scope for user_id and post_id
  if (CheckInShard(id, local_shard) == false) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: APPEND request server not responsible for id");
  }

  if (parsed.size() == 3 && (parsed[0] == "user" && parsed[2] == "posts")) {
    it = kv_store.find(key);
    if (it == kv_store.end()) {
      kv_store.insert(std::pair<std::string, std::string>(key, data));
    } else {
      it->second += data;
    }
    return ::grpc::Status::OK;
  }

  it = kv_store.find(key);
  if (it == kv_store.end()) {
    // if not found, we can only handle user_id here, cuz for post, we can't
    // create a post for a user we don't know
    if (parsed.size() == 2 && parsed[0] == "post") {
      return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                            "ERR: APPEND request cannot handle post_id without "
                            "user_id specified");
    } else if (parsed.size() == 2 && parsed[0] == "user") {
      kv_store.insert(std::pair<std::string, std::string>(key, data));
      // add to all_users both in map & in list
      it = kv_store.find("all_users");
      it->second = it->second + key + ","; // cat new "user_id,"
    }
  } else { // if user_id/post_id exists, just append data
    it->second = it->second + data;
  }
  return ::grpc::Status::OK;
}

/**
 * Deletes the key-value pair associated with this key from the server.
 * If this server does not contain the requested key, do nothing and return
 * the error specified
 *
 * @param context - you can ignore this
 * @param request A message containing the key to be removed
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Delete(::grpc::ServerContext *context,
                                     const ::DeleteRequest *request,
                                     Empty *response) {
  // what the client isn't authorized to delete:
  // all_users, user_id_posts
  // deleting a post would need to modify user_id_posts, but now we don't know
  // which user this post belongs to unless adding external ds to monitor it, so
  // skip for now deleting a user_id would (potentially) need to delete all
  // posts of the user
  std::string key = request->key();
  if (key == "") {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: DELETE request key null");
  }
  if (key == "all_users") {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: DELETE request all users illegal behavior");
  }
  std::vector<std::string> parsed = parse_value(key, "_");

  int id = extractID(key);
  // check if id is in local scope for user_id and post_id
  if (CheckInShard(id, local_shard) == false) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "ERR: DELETE request server not responsible for id");
  }

  std::lock_guard<std::mutex> lock(kv_mutex);
  std::map<std::string, std::string>::iterator it;

  // if the key is a post_id
  if (parsed[0] == "post" && parsed.size() == 2) {
    it = kv_store.find(key);
    if (it == kv_store.end()) {
      // first check if contained in the "deleted" list, if so, return OK
      for (auto & del : deleted) {
        if (key == del) {
          return ::grpc::Status::OK;
        }
      }
      return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                            "ERR: DELETE request post_id not found on server");
    } else { // if post found in local kv_store, delete from map & add to "deleted" list
      kv_store.erase(it);
      deleted.push_back(key);
    }
    return ::grpc::Status::OK;
  }

  // if the key is a user_id
  if (parsed[0] == "user" && parsed.size() == 2) { // if user_id
  it = kv_store.find(key);
  if (it == kv_store.end()) { // key not found on this server
      return ::grpc::Status(
          ::grpc::StatusCode::INVALID_ARGUMENT,
          "ERR: DELETE request user_id not found on server");
    }
    kv_store.erase(key);                           // delete the user
    it = kv_store.find(key + "_posts"); // deleting all posts associated with a
    // user if deleting a user_id

    std::vector<std::string> this_user_posts = parse_value(it->second, ",");
    for (auto &post :
         this_user_posts) { // delete all posts associated with this user, if
                            // post not found in local kv, then RPC delete on
                            // the server responsible
      it = kv_store.find(post);
      if (it == kv_store.end()) {

        int post_id = extractID(post);
        for (auto &server_shard : server_shard_map) {
          if (CheckInShard(post_id, server_shard.second) &&
              address != server_shard.first) {
            auto channel = grpc::CreateChannel(
                server_shard.first, grpc::InsecureChannelCredentials());
            auto stub = Shardkv::NewStub(channel);

            ::grpc::ClientContext cc;
            DeleteRequest req;
            Empty res;
            req.set_key(post);

            auto status = stub->Delete(&cc, req, &res);
            while (!status.ok()) { // sleep & retry till success
              std::chrono::milliseconds timespan(50);
              std::this_thread::sleep_for(timespan);
              ::grpc::ClientContext new_cc;
              status = stub->Delete(&new_cc, req, &res);
            }
          }
        }
      } else { // if post found in local kv_store, delete from map
        kv_store.erase(it);
        deleted.push_back(key);
      }
    }
    it = kv_store.find("all_users");
    std::vector<std::string> all_users = parse_value(it->second, ",");
    int m = 0;
    for (m; m < all_users.size(); m++) {
      if (key == all_users.at(m)) {
        break;
      }
    }
    all_users.erase(all_users.begin() + m); // erase user from all_users list
    std::string new_all_users = "";
    // join the string back together
    for (auto &c : all_users) {
      new_all_users = new_all_users + c + ",";
    }
    it->second = new_all_users;

    kv_store.erase(key + "_posts"); // delete user_id_posts too
  }
   return ::grpc::Status::OK;
}

/**
 * This method is called in a separate thread on periodic intervals (see the
 * constructor in shardkv.h for how this is done). It should query the
 * shardmaster for an updated configuration of how shards are distributed. You
 * should then find this server in that configuration and look at the shards
 * associated with it. These are the shards that the shardmaster deems this
 * server responsible for. Check that every key you have stored on this server
 * is one that the server is actually responsible for according to the
 * shardmaster. If this server is no longer responsible for a key, you should
 * find the server that is, and call the Put RPC in order to transfer the
 * key/value pair to that server. You should not let the Put RPC fail. That is,
 * the RPC should be continually retried until success. After the put RPC
 * succeeds, delete the key/value pair from this server's storage. Think about
 * concurrency issues like potential deadlock as you write this function!
 *
 * @param stub a grpc stub for the shardmaster, which we use to invoke the Query
 * method!
 */
void ShardkvServer::QueryShardmaster(Shardmaster::Stub *stub) {
  Empty query;
  QueryResponse response;
  ::grpc::ClientContext cc;

  auto status = stub->Query(&cc, query, &response);
  // now we have the server addr, the shards this server is responsible for (in
  // config from response)
  if (status.ok()) {
    int server_num = response.config_size();

    kv_mutex.lock();
    server_shard_map.clear();

    for (int i = 0; i < server_num; i++) {
      ConfigEntry *config = response.mutable_config(i);
      std::string server_addr = config->server();
      std::vector<shard> server_shards;
      int shards_num = config->shards_size();
      for (int j = 0; j < shards_num; j++) {
        Shard *shard = config->mutable_shards(j);
        shard_t s = shard_t();
        s.lower = shard->lower();
        s.upper = shard->upper();
        server_shards.push_back(s);
      }
      server_shard_map.insert(std::pair<std::string, std::vector<shard>>(
          server_addr, server_shards));
    }
    std::map<std::string, std::vector<shard>>::iterator it;
    it = server_shard_map.find(address);
    // update current server's shard range
    local_shard = it->second;
    // unlock upon finish modifying the server: shards map
    // shard_mutex.unlock();

    // extra data structure that stores the new kv_mappings
    std::map<std::string, std::string> updated_kv_store;
    // kv_mutex.lock();
    for (auto &kv : kv_store) {
      printf("kv.first is: %s\n", kv.first.c_str());
      if (kv.first == "all_users") {
        continue;
      }
      int id = extractID(kv.first); // for user_id, post_id, and user_id_posts
      if (CheckInShard(id, local_shard) ==
          false) { // after updating local shards, if the key in map is no
                   // longer in scope, issue put request
        for (auto &server : server_shard_map) {
          if (CheckInShard(id, server.second)) {
            auto channel = grpc::CreateChannel(
                server.first, grpc::InsecureChannelCredentials());
            auto stub = Shardkv::NewStub(channel);

            ::grpc::ClientContext cc;
            PutRequest req;
            Empty res;
            req.set_key(kv.first);
            req.set_data(kv.second);
            req.set_user("");

            auto status = stub->Put(&cc, req, &res);
            while (!status.ok()) { // sleep & retry till success
              std::chrono::milliseconds timespan(50);
              std::this_thread::sleep_for(timespan);
              ::grpc::ClientContext new_cc;
              status = stub->Put(&new_cc, req, &res);
            }
          }
        }
        // modify the all_users for local server (if the key to be removed is a
        // user_id)
        std::vector<std::string> parsed = parse_value(kv.first, "_");
        if (parsed.size() == 2 && parsed[0] == "user") {
          std::map<std::string, std::string>::iterator kv_it;
          kv_it = kv_store.find("all_users");
          std::vector<std::string> all_users = parse_value(kv_it->second, ",");
          int m = 0;
          for (m; m < all_users.size(); m++) {
            if (kv.first == all_users.at(m)) {
              break;
            }
          }
          all_users.erase(all_users.begin() +
                          m); // erase user from all_users list
          std::string new_all_users = "";
          // join the string back together
          for (auto &c : all_users) {
            new_all_users = new_all_users + c + ",";
          }
          kv_it->second = new_all_users;
        }
      } else { // if the kv is in scope, we add it to the updated mapping
        updated_kv_store.insert(
            std::pair<std::string, std::string>(kv.first, kv.second));
      }
    }
    // find the original all_users mapping in kv_store, add to the
    // updated_mapping
    std::map<std::string, std::string>::iterator s_it;
    s_it = kv_store.find("all_users");
    updated_kv_store.insert(
        std::pair<std::string, std::string>(s_it->first, s_it->second));
    // clear the original map & assign it to the new mapping
    kv_store.clear();
    kv_store = updated_kv_store;

    kv_mutex.unlock();
  } else {
    printf("BAD STATUS :(");
  }
}
