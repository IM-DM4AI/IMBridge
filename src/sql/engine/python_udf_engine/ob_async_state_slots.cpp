#include "ob_async_state_slots.h"
#include "concurrentqueue.h"
#include "lightweightsemaphore.h"

typedef duckdb_moodycamel::ConcurrentQueue<int> slots_queue_t;

namespace oceanbase
{
    namespace sql
    {   
        std::mutex AsyncStateSlots::mutex_init;

        struct SlotsQueue{
            slots_queue_t queue;
            SlotsQueue(int slots_num) : queue(slots_num) {}
            bool try_dequeue(int &slot_id)
            {
                return queue.try_dequeue(slot_id);
            }

            bool enqueue(int slot_id)
            {
                return queue.enqueue(slot_id);
            }
        };

        AsyncStateSlots::AsyncStateSlots(int slots_num) : slots_num(slots_num) {
            id_queue = new SlotsQueue(slots_num);
        }
        AsyncStateSlots::~AsyncStateSlots()
        {
            // reset every ObPUInputStore and ObColInputStore
            for (int i = 0; i < slots_num; i++)
            {
                delete controller_slots[i];
            }
            controller_slots.clear();
            delete id_queue;
        }
        AsyncStateSlots *AsyncStateSlots::slots_instance = nullptr;
        AsyncStateSlots *AsyncStateSlots::GetOrCreateInstance(int slot_count)
        {
            if (slots_instance == nullptr)
            {
                std::lock_guard<std::mutex> lock(mutex_init);
                if (slots_instance == nullptr && slot_count != -1)
                {
                    slots_instance = new AsyncStateSlots(slot_count);
                }
            }
            return slots_instance;
        }
        void AsyncStateSlots::ResetInstance(){
            if(slots_instance != nullptr){
                std::lock_guard<std::mutex> lock(mutex_init);
                if(slots_instance != nullptr){
                    delete slots_instance;
                    slots_instance = nullptr;
                }
            }
        }
        int AsyncStateSlots::InitControllerSlots(int64_t batch_size,
                                const common::ObIArray<ObExpr *> &udf_exprs,
                                const common::ObIArray<ObExpr *> &input_exprs,
                                const uint64_t tenant_id)
        {
            int ret = OB_SUCCESS;
            std::lock_guard<std::mutex> lock(mutex_init);
            if (is_init == false)
            {
                for (int i = 0; i < slots_num && ret == OB_SUCCESS; i++)
                {
                    ObPUStoreController *slot = new ObPUStoreController();
                    int init_ret = slot->init(batch_size, udf_exprs, input_exprs, tenant_id);
                    if (init_ret != OB_SUCCESS)
                    {
                        ret = OB_NOT_INIT;
                        return ret;
                    }
                    controller_slots.push_back(slot);
                    set_slot_id_to_queue(i);
                }
                is_init = true;
            }
            return ret;
        }

        int AsyncStateSlots::get_slot_id_from_queue()
        {
            int slot_id;
            bool success = id_queue->try_dequeue(slot_id);
            return success ? slot_id : -1;
        }

        bool AsyncStateSlots::set_slot_id_to_queue(int id)
        {
            bool success = id_queue->enqueue(id);
            return success;
        }

    } // end namespace sql
} // end namespace oceanbase
