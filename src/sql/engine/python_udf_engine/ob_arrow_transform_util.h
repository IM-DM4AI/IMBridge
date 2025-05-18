#ifndef OCEANBASE_SUBQUERY_OB_ARROW_TRANSFORM_UTIL_H_
#define OCEANBASE_SUBQUERY_OB_ARROW_TRANSFORM_UTIL_H_

#include "ob_python_udf_op.h"

#include <arrow/api.h>

namespace oceanbase
{
    namespace sql
    {
        bool convert_ob_data_to_arrow(std::shared_ptr<arrow::Table> &table, ObPUInputStore &input, ObExpr *expr, int64_t idx, int64_t eval_size);

        bool convert_arrow_to_ob_data(std::shared_ptr<arrow::Table> &table, ObEvalCtx &eval_ctx, ObIVector *res, ObExpr *expr, int64_t idx, int64_t res_size);

        bool convert_arrow_to_ob_data(std::shared_ptr<arrow::Table> &table, ObEvalCtx &eval_ctx, ObDatum *res, ObExpr *expr, int64_t idx, int64_t res_size);
    } // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SUBQUERY_OB_ARROW_TRANSFORM_UTIL_H_