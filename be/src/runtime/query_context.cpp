// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "runtime/query_context.h"

namespace starrocks {

static constexpr size_t NUM_SLOTS = 16;

QueryContextManager::QueryContextManager() : _mutexes(NUM_SLOTS), _maps(NUM_SLOTS) {}
QueryContextManager::~QueryContextManager() {}

QueryContext* QueryContextManager::get_or_register(const starrocks::TUniqueId& k) {
    size_t i = std::hash<size_t>()(k.lo) % NUM_SLOTS;
    auto& mutex = _mutexes[i];
    auto& map = _maps[i];
    {
        std::shared_lock<std::shared_mutex> lock(mutex);
        auto it = map.find(k);
        if (it != map.end()) {
            it->second->increment_num_instances();
            return it->second.get();
        }
    }
    {
        std::unique_lock<std::shared_mutex> lock(mutex);
        auto it = map.find(k);
        if (it != map.end()) {
            it->second->increment_num_instances();
            return it->second.get();
        }

        auto&& ctx = std::make_shared<QueryContext>();
        auto* ctx_raw_ptr = ctx.get();
        ctx_raw_ptr->set_query_id(k);
        ctx_raw_ptr->increment_num_instances();
        map.emplace(k, std::move(ctx));
        return ctx_raw_ptr;
    }
}

void QueryContextManager::remove(const TUniqueId& k) {
    size_t i = std::hash<size_t>()(k.lo) % NUM_SLOTS;
    auto& mutex = _mutexes[i];
    auto& map = _maps[i];
    std::unique_lock<std::shared_mutex> lock(mutex);
    map.erase(k);
}

void QueryContextManager::clean_removable_query_contexts() {
    for (int i = 0; i < NUM_SLOTS; ++i) {
        auto& mutex = _mutexes[i];
        auto& map = _maps[i];
        vector<TUniqueId> _trash;
        {
            std::shared_lock<std::shared_mutex> lock(mutex);
            auto it = map.begin();
            while (it != map.end()) {
                if (it->second->is_removable()) {
                    _trash.push_back(it->second->query_id());
                }
                ++it;
            }
        }
        for (auto& query_id : _trash) {
            std::unique_lock<std::shared_mutex> lock(mutex);
            map.erase(query_id);
        }
    }
}
} // namespace starrocks