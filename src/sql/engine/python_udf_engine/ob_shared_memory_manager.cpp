#include "ob_shared_memory_manager.h"
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>

namespace bi = boost::interprocess;

namespace oceanbase
{
	namespace sql
	{
		class SharedMemoryManager::Impl
		{
		public:
			bi::managed_shared_memory segment;
			bi::interprocess_semaphore *sem_client;
			bi::interprocess_semaphore *sem_server;

			Impl(const std::string &channel_name, const size_t size)
				: segment(bi::open_or_create, channel_name.c_str(), size),
				  sem_client(segment.find_or_construct<bi::interprocess_semaphore>((channel_name + "client").c_str())(0)),
				  sem_server(segment.find_or_construct<bi::interprocess_semaphore>((channel_name + "server").c_str())(0))
			{
			}
		};

		SharedMemoryManager::SharedMemoryManager(const std::string &name, ProcessKind kind, const size_t size)
			: channel_name(name), kind(kind), size(size), impl(new Impl(name, size))
		{
		}

		SharedMemoryManager::~SharedMemoryManager()
		{
			if (kind == ProcessKind::MANAGER)
			{
				bi::shared_memory_object::remove(channel_name.c_str());
			}
			delete impl;
		}

		template <typename T>
		T *SharedMemoryManager::create_shared_memory_object(const std::string &name, const size_t size)
		{
			return impl->segment.construct<T>((channel_name + name).c_str())[size]();
		}

		template <typename T>
		std::pair<T *, size_t> SharedMemoryManager::open_shared_memory_object(const std::string &name)
		{
			return impl->segment.find<T>((channel_name + name).c_str());
		}

		template <typename T>
		void SharedMemoryManager::destroy_shared_memory_object(const std::string &name)
		{
			impl->segment.destroy<T>((channel_name + name).c_str());
		}

		template char *SharedMemoryManager::create_shared_memory_object<char>(const std::string &name, size_t size);
		template std::pair<char *, size_t> SharedMemoryManager::open_shared_memory_object<char>(const std::string &name);
		template void SharedMemoryManager::destroy_shared_memory_object<char>(const std::string &name);
	} // end namespace sql
} // end namespace oceanbase