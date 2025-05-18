#include "ob_arrow_transform_util.h"

#include <stdexcept>
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
        bool convert_ob_data_to_arrow(std::shared_ptr<arrow::Table> &table, ObPUInputStore &input, ObExpr *expr, int64_t idx, int64_t eval_size)
        {
            std::vector<std::shared_ptr<arrow::Array>> arrays;
            std::vector<std::shared_ptr<arrow::Field>> fields;
            for (int i = 0; i < expr->arg_cnt_; ++i)
            {
                std::shared_ptr<arrow::Array> array;
                std::string col_name = "column_" + std::to_string(i);
                switch (expr->args_[i]->datum_meta_.type_)
                {
                case ObCharType:
                case ObVarcharType:
                case ObTinyTextType:
                case ObTextType:
                case ObMediumTextType:
                case ObLongTextType:
                {
                    ObDatum *src = reinterpret_cast<ObDatum *>(input.get_data_ptr_at(i)) + idx;
                    arrow::StringBuilder builder;
                    for (int j = 0; j < eval_size; ++j)
                    {
                        arrow::Status status = builder.Append(src[j].ptr_, src[j].len_);
                    }
                    arrow::Status status = builder.Finish(&array);
                    fields.push_back(arrow::field(col_name, arrow::utf8()));
                    break;
                }
                case ObTinyIntType:
                case ObSmallIntType:
                case ObMediumIntType:
                case ObInt32Type:
                case ObIntType:
                {
                    auto buffer = arrow::Buffer::Wrap(
                        reinterpret_cast<const int32_t *>(input.get_data_ptr_at(i)) + idx, eval_size);
                    array = std::make_shared<arrow::Int32Array>(eval_size, buffer);
                    fields.push_back(arrow::field(col_name, arrow::int32()));
                    break;
                }
                case ObDoubleType:
                {
                    auto *data_ptr = reinterpret_cast<const double *>(input.get_data_ptr_at(i)) + idx;
                    auto buffer = arrow::Buffer::Wrap(reinterpret_cast<const uint8_t *>(data_ptr), eval_size * sizeof(double));
                    array = std::make_shared<arrow::DoubleArray>(eval_size, buffer);
                    fields.push_back(arrow::field(col_name, arrow::float64()));
                    break;
                }
                default:
                    return false;
                }
                arrays.push_back(array);
            }

            auto schema = std::make_shared<arrow::Schema>(fields);
            table = arrow::Table::Make(schema, arrays);
            /*
            // !!check table
            if (arrow_table)
            {
              std::ostringstream oss;

              arrow::PrettyPrintOptions options;
              options.indent = 2;
              options.window = 10;

              auto status = arrow::PrettyPrint(*arrow_table, options, &oss);
              std::string arrow_table_str = oss.str();
              LOG_INFO("[arrow table]_____________________________________table \n", KCSTRING(arrow_table_str.c_str()));
              LOG_INFO("[arrow table]_____________________________________compute finished\n", K(ret));
            }
            */
            return true;
        }

        bool convert_arrow_to_ob_data(std::shared_ptr<arrow::Table> &table, ObEvalCtx &eval_ctx, ObIVector *res, ObExpr *expr, int64_t idx, int64_t res_size)
        {
            std::shared_ptr<arrow::ChunkedArray> column = table->column(0);
            std::vector<std::shared_ptr<arrow::Array>> chunks = column->chunks();
            std::shared_ptr<arrow::Array> array = arrow::Concatenate(chunks).ValueOrDie();
            // 构造vector并赋回expr_
            switch (expr->datum_meta_.type_)
            {
            case ObCharType:
            case ObVarcharType:
            case ObTinyTextType:
            case ObTextType:
            case ObMediumTextType:
            case ObLongTextType:
            {
                auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
                for (int i = 0; i < res_size; ++i)
                {
                    const char *str_data = string_array->GetView(idx + i).data();
                    int64_t str_length = string_array->GetView(idx + i).size();
                    res->set_string(i, common::ObString(str_length, str_data));
                }
                break;
            }
            case ObTinyIntType:
            case ObSmallIntType:
            case ObMediumIntType:
            case ObInt32Type:
            case ObIntType:
            {
                auto int_array = std::static_pointer_cast<arrow::Int32Array>(array);
                for (int i = 0; i < res_size; ++i)
                {
                    res->set_int(i, int_array->Value(idx + i));
                }
                break;
            }
            case ObDoubleType:
            {
                auto double_array = std::static_pointer_cast<arrow::DoubleArray>(array);
                for (int i = 0; i < res_size; ++i)
                {
                    res->set_double(i, double_array->Value(idx + i));
                }
                break;
            }
            default:
            {
                return false;
            }
            }
            return true;
        }

        bool convert_arrow_to_ob_data(std::shared_ptr<arrow::Table> &table, ObEvalCtx &eval_ctx, ObDatum *res, ObExpr *expr, int64_t idx, int64_t res_size)
        {
            std::shared_ptr<arrow::Array> array = arrow::Concatenate(table->column(0)->chunks()).ValueOrDie();
            switch (OB_NOT_SUPPORTED)
            {
            case ObCharType:
            case ObVarcharType:
            case ObTinyTextType:
            case ObTextType:
            case ObMediumTextType:
            case ObLongTextType:
            {
                auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
                for (int i = 0; i < res_size; ++i)
                {
                    expr->reset_ptr_in_datum(eval_ctx, i);
                    const char *str_data = string_array->GetView(idx + i).data();
                    int64_t str_length = string_array->GetView(idx + i).size();
                    res[i].set_string(common::ObString(str_length, str_data));
                }
                break;
            }
            case ObTinyIntType:
            case ObSmallIntType:
            case ObMediumIntType:
            case ObInt32Type:
            case ObIntType:
            {
                auto int_array = std::static_pointer_cast<arrow::Int32Array>(array);
                for (int i = 0; i < res_size; ++i)
                {
                    expr->reset_ptr_in_datum(eval_ctx, i);
                    res[i].set_int(int_array->Value(idx + i));
                }
                break;
            }
            case ObDoubleType:
            {
                auto double_array = std::static_pointer_cast<arrow::DoubleArray>(array);
                for (int i = 0; i < res_size; ++i)
                {
                    expr->reset_ptr_in_datum(eval_ctx, i);
                    res[i].set_double(double_array->Value(idx + i));
                }
                break;
            }
            default:
            {
                return false;
            }
            }
            return true;
        }
    } // end namespace sql
} // end namespace oceanbase