#ifndef OCEANBASE_SUBQUERY_OB_ASYNC_STATE_SLOTS_H_
#define OCEANBASE_SUBQUERY_OB_ASYNC_STATE_SLOTS_H_

#include "ob_python_udf_op.h"
#include <vector>
#include <mutex>
#include <future>

namespace oceanbase
{
    namespace sql
    {
        struct SlotsQueue;
        class AsyncStateSlots
        {
        public:
            AsyncStateSlots(int slots_num);
            ~AsyncStateSlots();
            static AsyncStateSlots *GetOrCreateInstance(int slot_count = -1);
            static void ResetInstance();
            int InitControllerSlots(int64_t batch_size,
                                    const common::ObIArray<ObExpr *> &udf_exprs,
                                    const common::ObIArray<ObExpr *> &input_exprs,
                                    const uint64_t tenant_id);

            int get_slot_id_from_queue();

            bool set_slot_id_to_queue(int id);

            static AsyncStateSlots *slots_instance; // Single case mode
            static std::mutex mutex_init; // init lock
            std::mutex mutex_controller;
            std::mutex res_save;
            bool is_init = false; // init tag
            std::vector<ObPUStoreController *> controller_slots; // slots
            std::vector<std::future<int>*> res_collect; // async res
            int slots_num; // count of slots
            SlotsQueue *id_queue;
        };
    } // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SUBQUERY_OB_ASYNC_STATE_SLOTS_H_