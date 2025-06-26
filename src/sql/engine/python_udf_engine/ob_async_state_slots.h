#ifndef OCEANBASE_SUBQUERY_OB_ASYNC_STATE_SLOTS_H_
#define OCEANBASE_SUBQUERY_OB_ASYNC_STATE_SLOTS_H_

#include "ob_python_udf_op.h"
#include <vector>
#include <mutex>
#include <memory>
#include <future>
#include <atomic>

namespace oceanbase
{
    namespace sql
    {
        struct SlotsQueue;
        class AsyncStateSlots
        {
        public:
            AsyncStateSlots(int slots_num);
            static std::unique_ptr<AsyncStateSlots> &GetOrCreateInstance(int slot_count = -1);
            static void ResetInstance();
            int InitControllerSlots(int64_t batch_size,
                                    const common::ObIArray<ObExpr *> &udf_exprs,
                                    const common::ObIArray<ObExpr *> &input_exprs,
                                    const uint64_t tenant_id);

            int get_slot_id_from_queue();

            bool set_slot_id_to_queue(int id);

            int get_future_res();

            static std::unique_ptr<AsyncStateSlots> slots_instance; 
            static std::mutex mutex_init; // init lock
            static std::atomic<int> destroy_cnt;
            std::mutex mutex_controller;
            std::mutex res_save;
            bool is_init = false; // init tag
            std::vector<std::unique_ptr<ObPUStoreController>> controller_slots; // slots
            std::vector<std::unique_ptr<std::future<int>>> res_collect; // async res
            int slots_num; // count of slots
            
            std::unique_ptr<SlotsQueue> id_queue;
        };
    } // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SUBQUERY_OB_ASYNC_STATE_SLOTS_H_