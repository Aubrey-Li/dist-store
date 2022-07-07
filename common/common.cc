#include "common.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <regex>
#include <math.h>

void sortAscendingInterval(std::vector<shard_t> &shards) {
  std::sort(
      shards.begin(), shards.end(),
      [](const shard_t &a, const shard_t &b) { return a.lower < b.lower; });
}

size_t size(const shard_t &s) { return s.upper - s.lower + 1; }

std::pair<shard_t, shard_t> split_shard(const shard_t &s) {
  // can't get midpoint of size 1 shard
  assert(s.lower < s.upper);
  unsigned int midpoint = s.lower + ((s.upper - s.lower) / 2);
  return std::make_pair<shard_t, shard_t>({s.lower, midpoint},
                                          {midpoint + 1, s.upper});
}

// helper partition function, tested
std::vector<shard_t> partition(int n, unsigned int min, unsigned int max) {
  if (n < 1) {
    printf("ERR: can't partition less than 1");
    std::vector<shard> empty;
    return empty;
  }
  if (n == 1) {
    std::vector<shard_t> shards;
    shard s = shard_t();
    s.lower = min;
    s.upper = max;
    shards.push_back(s);
    return shards;
  }
  // if even partition
  if (n % 2 == 0) {
    std::vector<shard_t> shards;
    shard s = shard_t();
    s.lower = min;
    s.upper = max;
    shards.push_back(s);
    int i = 0;
    while (i < n / 2) {
      std::vector<shard_t> stack;
      while (!shards.empty()) {
        shard_t shard = shards.back();
        std::pair<shard_t, shard_t> pair = split_shard(shard);
        stack.push_back(pair.first);
        stack.push_back(pair.second);
        shards.pop_back();
      }
      shards = stack;
      i++;
    }
    sortAscendingInterval(shards);
    return shards;
  }
  else {
    // if odd partition
    int interval = floor((max - min) / n);
    std::vector<shard_t> shards;
    shard s = shard_t();
    s.lower = interval + 1;
    s.upper = max;
    shards.push_back(s);
    int i = 0;
    while (i < ((n-1) / 2)) {
      std::vector<shard_t> stack;
      while (!shards.empty()) {
        shard_t shard = shards.back();
        std::pair<shard_t, shard_t> pair = split_shard(shard);
        stack.push_back(pair.first);
        stack.push_back(pair.second);
        shards.pop_back();
      }
      shards = stack;
      i++;
    }
    shard ss = shard_t();
    ss.lower = min;
    ss.upper = interval;
    shards.push_back(ss);
    sortAscendingInterval(shards);
    return shards;
  }
}

void sortAscendingSize(std::vector<shard_t> &shards) {
  std::sort(
      shards.begin(), shards.end(),
      [](const shard_t &a, const shard_t &b) { return size(a) < size(b); });
}

void sortDescendingSize(std::vector<shard_t> &shards) {
  std::sort(
      shards.begin(), shards.end(),
      [](const shard_t &a, const shard_t &b) { return size(b) < size(a); });
}

size_t shardRangeSize(const std::vector<shard_t> &vec) {
  size_t tot = 0;
  for (const shard_t &s : vec) {
    tot += size(vec);
  }
  return tot;
}

OverlapStatus get_overlap(const shard_t &a, const shard_t &b) {
  if (a.upper < b.lower || b.upper < a.lower) {
    /**
     * A: [-----]
     * B:         [-----]
     */
    return OverlapStatus::NO_OVERLAP;
  } else if (b.lower <= a.lower && a.upper <= b.upper) {
    /**
     * A:    [----]
     * B:  [--------]
     * Note: This also includes the case where the two shards are equal!
     */
    return OverlapStatus::COMPLETELY_CONTAINED;
  } else if (a.lower < b.lower && a.upper > b.upper) {
    /**
     * A: [-------]
     * B:   [---]
     */
    return OverlapStatus::COMPLETELY_CONTAINS;
  } else if (a.lower >= b.lower && a.upper > b.upper) {
    /**
     * A:    [-----]
     * B: [----]
     */
    return OverlapStatus::OVERLAP_START;
  } else if (a.lower < b.lower && a.upper <= b.upper) {
    /**
     * A: [-------]
     * B:    [------]
     */
    return OverlapStatus::OVERLAP_END;
  } else {
    throw std::runtime_error("bad case in get_overlap\n");
  }
}

std::vector<std::string> split(const std::string &s) {
  std::vector<std::string> v;
  std::regex ws_re("\\s+"); // whitespace
  std::copy(std::sregex_token_iterator(s.begin(), s.end(), ws_re, -1),
            std::sregex_token_iterator(), std::back_inserter(v));
  return v;
}

std::vector<std::string> parse_value(std::string val, std::string delim) {
  std::vector<std::string> tokens;

  char *save;
  char *tok = strtok_r((char *)val.c_str(), delim.c_str(), &save);
  while (tok != NULL) {
    tokens.push_back(std::string(tok));
    tok = strtok_r(NULL, delim.c_str(), &save);
  }

  return tokens;
}

int extractID(std::string key) {
  std::vector<std::string> tokens;

  char *save;
  char *tok = strtok_r((char *)key.c_str(), "_", &save);
  while (tok != NULL) {
    tokens.push_back(std::string(tok));
    tok = strtok_r(NULL, "_", &save);
  }

  assert(tokens.size() > 1); // illformed key

  return stoi(tokens[1]);
}

// helper that checks if key is in this list of shards
bool CheckInShard(int key, std::vector<shard> shards) {
  int shard_intervals = shards.size();

  for (int i = 0; i < shard_intervals; i++) {
    shard s = shards.at(i);
    if (key >= s.lower && key <= s.upper) {
      return true;
    }
  }
  return false;
}