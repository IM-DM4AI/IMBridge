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

        struct AsyncScheduler;

        class IMLaneScheduler
        {
        public:
            IMLaneScheduler(bool is_manager = false, int sys_core = 0, int lane_id = -100,
                            const size_t size = SHARED_MEMORY_SIZE);

            ~IMLaneScheduler();

            void launch(float threshold = 0.7, int process_num = -1);

            void destroy();

            void reset_cache();

            void push_id_to_avaliable_queue(int id);

            void push_cmd_to_task_queue(int shm_id, int cmd);

            int get_message_from_task_queue();

            int get_id_from_avaliable_queue();

            bool check_use_async();

            int get_max_process();

            static std::unique_ptr<IMLaneScheduler>& GetOrCreateInstance(bool is_manager = false, int sys_core = 0, int lane_id = -100,
                            const size_t size = SHARED_MEMORY_SIZE);

            int warm_up = 1000; // ms
            // Use for async task
            std::unique_ptr<AsyncScheduler> async_scheduler;

        private:
            static std::unique_ptr<IMLaneScheduler> scheduler_instance; 
            class Impl;
            Impl *impl;
            int sys_cpu_core_nums = 0;
            bool is_manager;
            // only manager use
            std::atomic<int> working_threads_num;
            int total_threads_num;
            bool use_async = false;
            double async_threshold = 0.7;
            
        };
    } // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SUBQUERY_OB_UDF_SCHEDULER_H_