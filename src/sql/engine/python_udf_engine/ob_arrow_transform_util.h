#ifndef OCEANBASE_SUBQUERY_OB_ARROW_TRANSFORM_UTIL_H_
#define OCEANBASE_SUBQUERY_OB_ARROW_TRANSFORM_UTIL_H_


// #include "ob_shared_memory_manager.h"
#include "ob_python_udf_op.h"

#include <arrow/api.h>
// #include <boost/interprocess/managed_shared_memory.hpp>

// namespace bi = boost::interprocess;

namespace oceanbase
{
namespace sql
{
const std::string INPUT_TABLE = "INPUT_TABLE";
const std::string OUTPUT_TABLE = "OUTPUT_TABLE";

 bool convert_ob_data_to_arrow(std::shared_ptr<arrow::Table> &table, ObPUInputStore &input, ObExpr * expr, int64_t idx, int64_t eval_size);

// void WriteArrowTableToSharedMemory(std::shared_ptr<arrow::Table> &table, SharedMemoryManager &shm,
//                                    const std::string &shm_id = INPUT_TABLE);

// std::shared_ptr<arrow::Table> ReadArrowTableFromSharedMemory(SharedMemoryManager &shm,
//                                                              const std::string &shm_id = OUTPUT_TABLE);

bool convert_arrow_to_ob_data(std::shared_ptr<arrow::Table> &table, ObEvalCtx &eval_ctx, ObIVector *res, ObExpr * expr, int64_t idx, int64_t res_size);

bool convert_arrow_to_ob_data(std::shared_ptr<arrow::Table> &table, ObEvalCtx &eval_ctx, ObDatum *res, ObExpr * expr, int64_t idx, int64_t res_size);
} // end namespace sql
} // end namespace oceanbase


#endif // OCEANBASE_SUBQUERY_OB_ARROW_TRANSFORM_UTIL_H_