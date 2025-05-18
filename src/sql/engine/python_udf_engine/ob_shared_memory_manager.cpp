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

		void SharedMemoryManager::client_wait()
		{
			impl->sem_client->wait();
		}

		void SharedMemoryManager::client_post()
		{
			impl->sem_client->post();
		}

		void SharedMemoryManager::server_wait()
		{
			impl->sem_server->wait();
		}

		void SharedMemoryManager::server_post()
		{
			impl->sem_server->post();
		}

		bool write_arrow_to_shared_memory(std::shared_ptr<arrow::Table> &table, SharedMemoryManager &shm,
										  const std::string &shm_id)
		{
			std::shared_ptr<arrow::Buffer> buffer;
			int col = table->num_columns(), row = table->num_rows();
			auto bit = sizeof(double_t);
			std::shared_ptr<arrow::io::BufferOutputStream> stream =
				arrow::io::BufferOutputStream::Create(table->num_columns() * table->num_rows() * sizeof(double_t)).ValueOrDie();
			std::shared_ptr<arrow::ipc::RecordBatchWriter> writer =
				arrow::ipc::MakeStreamWriter(stream, table->schema()).ValueOrDie();
			auto write_status = writer->WriteTable(*table);
			if (!write_status.ok())
			{
				return false;
			}
			auto close_status = writer->Close();
			if (!close_status.ok())
			{
				return false;
			}
			buffer = stream->Finish().ValueOrDie();

			char *shm_ptr = shm.create_shared_memory_object<char>(shm_id, buffer->size());
			if (shm_ptr == nullptr)
			{
				return false;
			}
			std::memcpy(shm_ptr, buffer->data(), buffer->size());
			return true;
		}

		bool read_arrow_from_shared_memory(std::shared_ptr<arrow::Table> &table, SharedMemoryManager &shm,
										   const std::string &shm_id)
		{
			auto shm_table_pair = shm.open_shared_memory_object<char>(shm_id);

			if (shm_table_pair.first == nullptr)
			{
				return false;
			}

			char *shm_table_ptr = shm_table_pair.first;
			size_t shm_table_size = shm_table_pair.second;

			std::shared_ptr<arrow::Buffer> buffer = arrow::Buffer::Wrap(shm_table_ptr, shm_table_size);
			std::shared_ptr<arrow::io::InputStream> input = std::make_shared<arrow::io::BufferReader>(buffer);
			std::shared_ptr<arrow::ipc::RecordBatchReader> reader =
				arrow::ipc::RecordBatchStreamReader::Open(input).ValueOrDie();

			std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
			while (true)
			{
				std::shared_ptr<arrow::RecordBatch> batch = reader->ReadNext().ValueOrDie().batch;
				if (batch == nullptr)
				{
					break;
				}
				batches.push_back(batch);
			}

			table = arrow::Table::FromRecordBatches(reader->schema(), batches).ValueOrDie();
			return true;
		}

		template char *SharedMemoryManager::create_shared_memory_object<char>(const std::string &name, size_t size);
		template std::pair<char *, size_t> SharedMemoryManager::open_shared_memory_object<char>(const std::string &name);
		template void SharedMemoryManager::destroy_shared_memory_object<char>(const std::string &name);
	} // end namespace sql
} // end namespace oceanbase