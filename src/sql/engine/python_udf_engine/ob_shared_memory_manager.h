#ifndef OCEANBASE_SUBQUERY_OB_SHARED_MEMORY_MANAGER_H_
#define OCEANBASE_SUBQUERY_OB_SHARED_MEMORY_MANAGER_H_

#include <string>
#include <utility>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/writer.h>
#include <arrow/status.h>
#include <arrow/array/concatenate.h>

namespace oceanbase
{
    namespace sql
    {
        enum class ProcessKind : u_int8_t
        {
            CLIENT = 0,
            SERVER = 1,
            MANAGER = 2
        };
        const std::string INPUT_TABLE = "INPUT_TABLE";
        const std::string OUTPUT_TABLE = "OUTPUT_TABLE";

        class SharedMemoryManager
        {
        public:
            SharedMemoryManager(const std::string &name, ProcessKind process_kind, const size_t size = 1024 * 1024 * 32);
            ~SharedMemoryManager();

            template <typename T>
            T *create_shared_memory_object(const std::string &name, const size_t size);

            template <typename T>
            std::pair<T *, size_t> open_shared_memory_object(const std::string &name);

            template <typename T>
            void destroy_shared_memory_object(const std::string &name);

            void client_wait();

            void server_wait();

            void client_post();

            void server_post();

            std::string get_channel_name()
            {
                return channel_name;
            }
            class Impl;
            Impl *impl;

        private:
            std::string channel_name;
            ProcessKind kind;
            size_t size;
        };

        bool write_arrow_to_shared_memory(std::shared_ptr<arrow::Table> &table, SharedMemoryManager &shm,
                                          const std::string &shm_id = INPUT_TABLE);

        bool read_arrow_from_shared_memory(std::shared_ptr<arrow::Table> &table, SharedMemoryManager &shm,
                                           const std::string &shm_id = OUTPUT_TABLE);
    } // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SUBQUERY_OB_SHARED_MEMORY_MANAGER_H_