#ifndef OCEANBASE_SUBQUERY_OB_UDF_SCHEDULER_H_
#define OCEANBASE_SUBQUERY_OB_UDF_SCHEDULER_H_

#include <atomic>
#include <vector>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_map>
#include "concurrentqueue.h"
#include "lightweightsemaphore.h"
typedef duckdb_moodycamel::ConcurrentQueue<std::function<void()>> queue_t;
typedef duckdb_moodycamel::LightweightSemaphore semaphore_t;
namespace oceanbase
{
    namespace sql
    {
        // [STATE] use for udf server execute
        const int TASK_UDF_INFER = 106;
        const int TASK_DESTROY = 107;
        const int TASK_RESET_CACHE = 108;

        // config
        const int AVALIABLE_QUEUE_MESSAGE_MAX_SIZE = 256;
        const int TASK_QUEUE_MESSAGE_MAX_SIZE = 8;
        const int SHARED_MEMORY_SIZE = 1024 * 1024;

        const std::string PRE_FIX = "imlane_";
        const std::string TASK_QUEUE_NAME = PRE_FIX + "task_queue";
        const std::string AVALIABLE_QUEUE_NAME = PRE_FIX + "avaliable_queue";

        const std::string START_SERVER_COMMAND = "/home/IMBridge/imlane_server/server_start.sh  ";
        const int BASE_ID = 100000;

        struct AsyncScheduler
        {
            int sys_cpu_core_nums;
            std::vector<std::unique_ptr<std::thread>> async_workers;
            std::unique_ptr<queue_t> async_queue;
            std::unique_ptr<semaphore_t> async_semaphore;
            std::atomic<bool> async_stop;

            AsyncScheduler(int sys_core) : sys_cpu_core_nums(sys_core), async_stop(false)
            {
                async_semaphore = std::make_unique<semaphore_t>();
                async_queue = std::make_unique<queue_t>(sys_cpu_core_nums);
                for (size_t i = 0; i < sys_cpu_core_nums; i++)
                {
                    async_workers.emplace_back(std::make_unique<std::thread>([this]
                                                                             {
				while (!this->async_stop) {
					this->async_semaphore->wait();
					std::function<void()> task;
					if (this->async_queue->try_dequeue(task)) {
						task();
					}
				} }));
                }
            }
            ~AsyncScheduler()
            {
                async_stop = true;
                async_semaphore->signal(async_workers.size());
                for (auto &worker : async_workers)
                {
                    worker->join();
                }
                async_workers.clear();
            }

            template <class F, class... Args>
            auto enqueue(F &&f, Args &&...args) -> std::unique_ptr<std::future<typename std::result_of<F(Args...)>::type>>
            {
                using returnType = typename std::result_of<F(Args...)>::type;

                if (async_stop)
                {
                    throw std::runtime_error("enqueue on stopped ThreadPool");
                }

                // 使用 std::bind 替代 std::apply
                auto task = std::make_shared<std::packaged_task<returnType()>>(
                    std::bind(std::forward<F>(f), std::forward<Args>(args)...));

                bool success = async_queue->enqueue([task]()
                                                    { (*task)(); });
                if (success)
                {
                    async_semaphore->signal();
                    return std::make_unique<std::future<returnType>>(task->get_future());
                }
                return nullptr;
            }
        };

        class IMLaneScheduler
        {
        public:
            IMLaneScheduler(bool is_manager = false, int sys_core = 0, int lane_id = -100,
                            const size_t size = SHARED_MEMORY_SIZE);

            ~IMLaneScheduler();

            void launch(float threshold = 0.9, int process_num = -1);

            void destroy();

            void reset_cache();

            void push_id_to_avaliable_queue(int id);

            void push_cmd_to_task_queue(int shm_id, int cmd);

            int get_message_from_task_queue();

            int get_id_from_avaliable_queue();

            bool check_use_async();

            int get_max_process();


            static std::unique_ptr<IMLaneScheduler>& GetOrCreateInstance(bool is_manager = false, int sys_core = 0, float threshold = 0.7, int lane_id = -100,
                            const size_t size = SHARED_MEMORY_SIZE);

            int warm_up = 1000; // ms
            // Use for async task
            std::unique_ptr<AsyncScheduler> async_scheduler;
            // cpu cores count
            int sys_cpu_core_nums = 0;
            std::atomic<int> working_threads_num;
            bool use_async = false;
        private:
            static std::unique_ptr<IMLaneScheduler> scheduler_instance; 
            class Impl;
            Impl *impl;
            bool is_manager;
            // only manager use
            int total_threads_num;
            double async_threshold = 0.7;
            
        };
    } // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SUBQUERY_OB_UDF_SCHEDULER_H_