#include "ob_udf_scheduler.h"

#include <boost/interprocess/containers/deque.hpp>
#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <fstream>
#include <string>

namespace bi = boost::interprocess;

namespace oceanbase
{
    namespace sql
    {
        class IMLaneScheduler::Impl
        {
        public:
            bi::message_queue avaliable_queue;
            // only manager use
            std::unordered_map<int, std::unique_ptr<bi::message_queue>> all_task_queues;
            // only not manager use
            std::unique_ptr<bi::message_queue> task_queue;
            std::string lane_id_str;
            Impl(bool is_manager, int lane_id) : avaliable_queue(bi::open_or_create, AVALIABLE_QUEUE_NAME.c_str(), AVALIABLE_QUEUE_MESSAGE_MAX_SIZE, sizeof(int))
            {
                if (!is_manager)
                {
                    lane_id_str = TASK_QUEUE_NAME + std::to_string(lane_id);
                    task_queue = std::make_unique<bi::message_queue>(bi::open_or_create, lane_id_str.c_str(), TASK_QUEUE_MESSAGE_MAX_SIZE, sizeof(int));
                }
            }
        };

        IMLaneScheduler::IMLaneScheduler(bool is_manager, int sys_core, int lane_id,
                                         const size_t size) : is_manager(is_manager), working_threads_num(0), total_threads_num(sys_core), impl(new Impl(is_manager, lane_id))
        {
            if (is_manager)
            {
                sys_cpu_core_nums = sys_core;
            }
        }
        IMLaneScheduler::~IMLaneScheduler()
        {
            destroy();
        }
        void IMLaneScheduler::launch(float threshold, int process_num)
        {
            if (is_manager)
            {
                if (threshold > 0.0 && threshold < 1.0)
                {
                    async_threshold = threshold;
                }
                if (process_num > 0 && process_num <= sys_cpu_core_nums)
                {
                    total_threads_num = process_num;
                }
                async_scheduler = std::make_unique<AsyncScheduler>(total_threads_num);
                for (int i = BASE_ID; i < BASE_ID + total_threads_num; i++)
                {
                    std::string lane_id = std::to_string(i);
                    auto tq = std::make_unique<bi::message_queue>(bi::open_or_create, (TASK_QUEUE_NAME + lane_id).c_str(),
                                                                  TASK_QUEUE_MESSAGE_MAX_SIZE, sizeof(int));
                    impl->all_task_queues[i] = std::move(tq);
                    std::string cmd = START_SERVER_COMMAND + lane_id;
                    std::thread t([cmd]()
                                  { std::system(cmd.c_str()); });
                    t.detach();
                }
            }
        }
        void IMLaneScheduler::destroy()
        {
            if (is_manager)
            {
                for (auto &pair : impl->all_task_queues)
                {
                    auto &id = pair.first;
                    auto &tq = pair.second;
                    int destroy_command = TASK_DESTROY;
                    tq->send(&destroy_command, sizeof(int), 0);
                }
                impl->all_task_queues.clear();
                async_scheduler.reset();
                bi::message_queue::remove(AVALIABLE_QUEUE_NAME.c_str());
            }
            else
            {
                bi::message_queue::remove(impl->lane_id_str.c_str());
            }
            delete impl;
        }
        void IMLaneScheduler::reset_cache()
        {
            if (is_manager)
            {
                int reset_cache = TASK_RESET_CACHE;
                for (auto &pair : impl->all_task_queues)
                {
                    auto &id = pair.first;
                    auto &tq = pair.second;
                    tq->send(&reset_cache, sizeof(int), 0);
                }
            }
        }
        void IMLaneScheduler::push_id_to_avaliable_queue(int id)
        {
            impl->avaliable_queue.send(&id, sizeof(int), 0);
            working_threads_num--;
        }
        void IMLaneScheduler::push_cmd_to_task_queue(int shm_id, int cmd)
        {
            impl->all_task_queues[shm_id]->send(&cmd, sizeof(int), 0);
        }
        int IMLaneScheduler::get_message_from_task_queue()
        {
            int msg;
            bi::message_queue::size_type recvd_size;
            unsigned int priority;
            impl->task_queue.get()->receive(&msg, sizeof(int), recvd_size, priority);
            return msg;
        }
        int IMLaneScheduler::get_id_from_avaliable_queue()
        {
            int msg;
            bi::message_queue::size_type recvd_size;
            unsigned int priority;
            impl->avaliable_queue.receive(&msg, sizeof(int), recvd_size, priority);
            working_threads_num++;
            return msg;
        }
        bool IMLaneScheduler::check_use_async()
        {
            if (working_threads_num < total_threads_num * async_threshold)
            {
                use_async = true;
            }
            return use_async;
        }
        int IMLaneScheduler::get_max_process()
        {
            return total_threads_num;
        }
        std::unique_ptr<IMLaneScheduler> IMLaneScheduler::scheduler_instance = nullptr;
        std::unique_ptr<IMLaneScheduler> &IMLaneScheduler::GetOrCreateInstance(bool is_manager, int sys_core, float threshold, int lane_id,
                                                                               const size_t size)
        {
            if (IMLaneScheduler::scheduler_instance.get() == nullptr)
            {
                if (is_manager)
                {
                    int t_sys_core = std::thread::hardware_concurrency();
                    const std::string quota_path = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
                    const std::string period_path = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";

                    std::ifstream quota_file(quota_path);
                    std::ifstream period_file(period_path);
                    if (quota_file.is_open() && period_file.is_open())
                    {
                        long long quota, period;
                        quota_file >> quota;
                        period_file >> period;

                        if (quota > 0 && period > 0)
                        {
                            t_sys_core = std::min(static_cast<int>(quota / period), t_sys_core);
                        }
                    }
                    t_sys_core = std::max(t_sys_core, 1);
                    sys_core = sys_core > 0 ? std::min(sys_core, t_sys_core) : t_sys_core;
                }
                IMLaneScheduler::scheduler_instance = std::make_unique<IMLaneScheduler>(is_manager, sys_core, lane_id, size);
                IMLaneScheduler::scheduler_instance->launch(threshold, sys_core);
            }
            return IMLaneScheduler::scheduler_instance;
        }
    } // end namespace sql
} // end namespace oceanbase