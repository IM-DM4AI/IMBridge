#include "ob_async_state_slots.h"
#include "concurrentqueue.h"
#include "lightweightsemaphore.h"

typedef duckdb_moodycamel::ConcurrentQueue<int> slots_queue_t;

namespace oceanbase
{
    namespace sql
    {   
        std::mutex AsyncStateSlots::mutex_init;
        std::atomic<int> AsyncStateSlots::destroy_cnt(0);

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
            id_queue = std::unique_ptr<SlotsQueue>(new SlotsQueue(slots_num));
        }
        std::unique_ptr<AsyncStateSlots>  AsyncStateSlots::slots_instance = nullptr;
        std::unique_ptr<AsyncStateSlots> &AsyncStateSlots::GetOrCreateInstance(int slot_count)
        {
            if(slot_count != -1){ //create
                destroy_cnt++;
            }
            if (slots_instance == nullptr && slot_count != -1)
            {
                std::lock_guard<std::mutex> lock(mutex_init);
                if (slots_instance == nullptr)
                {
                    slots_instance = std::unique_ptr<AsyncStateSlots>(new AsyncStateSlots(slot_count));
                }
            }
            return slots_instance;
        }
        void AsyncStateSlots::ResetInstance(){
            destroy_cnt--;
            if(slots_instance != nullptr && slots_instance->destroy_cnt.load() == 0){
                std::lock_guard<std::mutex> lock(mutex_init);
                if(slots_instance != nullptr){
                    slots_instance.reset();
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
                    std::unique_ptr<ObPUStoreController> slot(new ObPUStoreController());
                    int init_ret = slot->init(batch_size, udf_exprs, input_exprs, tenant_id);
                    if (init_ret != OB_SUCCESS)
                    {
                        ret = OB_NOT_INIT;
                        return ret;
                    }
                    controller_slots.push_back(std::move(slot));
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

        int AsyncStateSlots::get_future_res()
        {
            int future_id = -1;
            std::lock_guard<std::mutex> lock(res_save);
            for (int i = 0; i < res_collect.size();)
            {
                auto &it = res_collect[i];
                if (it && it->valid())
                {
                    if (it->wait_for(std::chrono::seconds(0)) == std::future_status::ready)
                    {
                        future_id = it->get();
                        res_collect.erase(res_collect.begin() + i);
                        break;
                    }else{
                        i++;
                    }
                }
                else
                {
                    res_collect.erase(res_collect.begin() + i);
                }
            }
            return future_id;
        }

    } // end namespace sql
} // end namespace oceanbase
