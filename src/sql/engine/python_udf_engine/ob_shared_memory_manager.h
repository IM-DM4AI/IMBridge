#ifndef OCEANBASE_SUBQUERY_OB_SHARED_MEMORY_MANAGER_H_
#define OCEANBASE_SUBQUERY_OB_SHARED_MEMORY_MANAGER_H_

#include <string>
#include <utility>

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

            std::string get_channel_name()
            {
                return channel_name;
            }

        private:
            class Impl;
            Impl *impl;
            std::string channel_name;
            ProcessKind kind;
            size_t size;
        };

    } // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SUBQUERY_OB_SHARED_MEMORY_MANAGER_H_