// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "common/object_pool.h"
#include "gen_cpp/Types_types.h" // for TUniqueId
#include "runtime/descriptors.h"

namespace starrocks {
using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::duration_cast;

class QueryContext;
using QueryContextPtr = std::shared_ptr<QueryContext>;

class QueryContext {
public:
    void increment_num_instances() {
        extend_lifetime();
        ++_num_instances;
        ++_num_instances_received;
    }
    size_t decrement_num_instances() {
        extend_lifetime();
        return --_num_instances;
    }

    void set_query_id(const TUniqueId& query_id) { _query_id = query_id; }
    TUniqueId query_id() const { return _query_id; }

    ObjectPool* object_pool() { return &_object_pool; }
    void set_desc_tbl(DescriptorTbl* desc_tbl) {
        DCHECK(_desc_tbl == nullptr);
        _desc_tbl = desc_tbl;
    }
    DescriptorTbl* desc_tbl() const {
        DCHECK(_desc_tbl != nullptr);
        return _desc_tbl;
    }

    void set_expire_seconds(int expire_seconds) { _expire_seconds = seconds(expire_seconds); }

    // now time point pass by deadline point.
    bool is_expired() {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return now > _deadline;
    }

    // add expired seconds to deadline
    void extend_lifetime() {
        auto old_deadline = _deadline.load();
        auto new_deadline =
                duration_cast<milliseconds>(steady_clock::now().time_since_epoch() + _expire_seconds).count();
        if (new_deadline > old_deadline) {
            _deadline.compare_exchange_strong(old_deadline, new_deadline);
        }
    }

    void set_total_instances_received(size_t n) { _total_instances_received = n; }
    bool is_removable() {
        return _num_instances == 0 && (_num_instances_received == _total_instances_received || is_expired());
    }

private:
    starrocks::TUniqueId _query_id;
    std::atomic<size_t> _num_instances{0};
    std::atomic<size_t> _num_instances_received{std::numeric_limits<size_t>::max()};
    size_t _total_instances_received{0};
    ObjectPool _object_pool;
    DescriptorTbl* _desc_tbl;
    std::atomic<int64_t> _deadline{0};
    seconds _expire_seconds{300};
};

class QueryContextManager {
    DECLARE_SINGLETON(QueryContextManager);

public:
    QueryContext* get_or_register(const starrocks::TUniqueId& k);
    void remove(const TUniqueId& k);
    void clean_removable_query_contexts();

private:
    QueryContextManager(const QueryContextManager&) = delete;
    QueryContextManager(QueryContextManager&&) = delete;
    QueryContextManager& operator=(const QueryContextManager&) = delete;
    QueryContextManager& operator=(QueryContextManager&&) = delete;

    std::vector<std::shared_mutex> _mutexes;
    std::vector<std::unordered_map<starrocks::TUniqueId, QueryContextPtr>> _maps;
};
} // namespace starrocks