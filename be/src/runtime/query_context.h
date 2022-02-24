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
class QueryContext;
using QueryContextPtr = std::shared_ptr<QueryContext>;

class QueryContext {
public:
    void increment_num_instances() { ++_num_instances; }
    size_t decrement_num_instances() { return --_num_instances; }

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

private:
    starrocks::TUniqueId _query_id;
    size_t _num_instances{0};
    ObjectPool _object_pool;
    DescriptorTbl* _desc_tbl;
};

class QueryContextManager {
    DECLARE_SINGLETON(QueryContextManager);

public:
    QueryContext* get_or_register(const starrocks::TUniqueId& k);
    void unregister(const starrocks::TUniqueId& k);

private:
    QueryContextManager(const QueryContextManager&) = delete;
    QueryContextManager(QueryContextManager&&) = delete;
    QueryContextManager& operator=(const QueryContextManager&) = delete;
    QueryContextManager& operator=(QueryContextManager&&) = delete;

    std::vector<std::shared_mutex> _mutexes;
    std::vector<std::unordered_map<starrocks::TUniqueId, QueryContextPtr>> _maps;
};
} // namespace starrocks