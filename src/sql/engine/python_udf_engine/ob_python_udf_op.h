#ifndef OCEANBASE_SUBQUERY_OB_PYTHON_UDF_OP_H_
#define OCEANBASE_SUBQUERY_OB_PYTHON_UDF_OP_H_

#include "sql/engine/basic/ob_ra_datum_store.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/subquery/ob_subplan_scan_op.h"
#include "sql/engine/expr/ob_expr_python_udf.h"


#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <Python.h>
#include <numpy/arrayobject.h>
#include <numpy/npy_common.h>
#include <arrow/api.h>
#include <arrow/table.h>
//#include <Python.h>

namespace oceanbase
{
namespace sql
{

const int DEFAULT_PU_LENGTH = 8192;
const int BUFFER_MAG = 2;

// buffer for columns that are not evaluated by python udf
struct ObColInputStore
{
public:
  ObColInputStore() 
  : buf_alloc_(), tmp_alloc_(), exprs_(buf_alloc_), datums_copy_(buf_alloc_),
    length_(0), batch_size_(0), saved_size_(0), output_idx_(0), inited_(false)
  {}
  ~ObColInputStore() {
    free();
    datums_copy_.destroy();
    exprs_.destroy();
  }
  int init(const common::ObIArray<ObExpr *> &exprs,
           int64_t batch_size,
           int64_t length);
  int free();
  int reuse();
  int reset(int64_t length = 0);
  int resize(int64_t size);
  int save_batch(ObEvalCtx &eval_ctx, ObBatchRows &brs);
  int save_vector(ObEvalCtx &eval_ctx, ObBatchRows &brs);
  int load_batch(ObEvalCtx &eval_ctx, int64_t load_size);
  int load_vector(ObEvalCtx &eval_ctx, int64_t load_size);

private:
  common::ObArenaAllocator buf_alloc_; // for array and pointers
  common::ObArenaAllocator tmp_alloc_; // for data copy
  common::ObFixedArray<ObExpr *, common::ObIAllocator> exprs_;
  common::ObFixedArray<ObDatum *, common::ObIAllocator> datums_copy_; // 相当于UNIFORM格式
  int64_t length_; // 当前容量
  int64_t batch_size_;
  int64_t saved_size_;
  int64_t output_idx_;
  bool inited_;
};

// store python udf expr input, data is directly sent to python interpreter
struct ObPUInputStore
{
public:
  ObPUInputStore() : buf_alloc_(NULL), tmp_alloc_(), expr_(NULL), length_(0), data_ptrs_(NULL), saved_size_(0), inited_(false) {}
  ~ObPUInputStore() { free(); }
  int init(common::ObIAllocator *alloc, ObExpr *expr, int64_t length);
  int alloc_data_ptrs(int64_t length);
  int reuse();
  int reset(int64_t length = 0);
  int free();
  int save_row(ObEvalCtx &eval_ctx, int64_t row_idx); // save row
  int save_batch(ObEvalCtx &eval_ctx, ObBatchRows &brs); // save batch
  int save_vector(ObEvalCtx &eval_ctx, ObBatchRows &brs); // save vector
  // not support load functions
  char** get_data_ptrs() { return data_ptrs_; }
  void* get_data_ptr_at(int64_t i) { // use with expr datum type
    if (i >= 0 && OB_NOT_NULL(expr_) && i < expr_->arg_cnt_)
      return reinterpret_cast<void *>(data_ptrs_[i]);
    return nullptr;
  }
  int64_t get_saved_size() {return saved_size_;}

private:
  common::ObIAllocator *buf_alloc_; // input store allocator (FIFO allocator)
  common::ObArenaAllocator tmp_alloc_; // for data copy
  ObExpr *expr_; // Python UDF expr
  int64_t length_; // 当前容量
  char **data_ptrs_;
  int64_t saved_size_;
  bool inited_;
};

class ObPythonUDFCell : public common::ObDLinkBase<ObPythonUDFCell> {
public:
  ObPythonUDFCell() {}
  ~ObPythonUDFCell() {}
  int init(ObExpr *expr, 
           int64_t batch_size, 
           int64_t length,
           uint64_t tenant_id);
  int free();
  int reset(int64_t size) { return input_store_.reset(size); }
  int do_store(ObEvalCtx &eval_ctx, ObBatchRows &brs); // do real storing
  int do_process(); // do real processing
  int do_process_all(); // process all saved store at one time
  int do_restore(ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size);
  int do_restore_batch(ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size);
  int do_restore_vector(ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size);
  int do_restore_arrow_to_vector(ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size);
  int do_restore_arrow_to_batch(ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size);

  //计算过程  
  int wrap_input_numpy(PyObject *&pArgs, int64_t &eval_size); // wrap all args
  int wrap_input_numpy(PyObject *&pArgs, int64_t idx, int64_t predict_size, int64_t &eval_size); // warp args in [idx, idx + predict_size]
  int eval(PyObject *pArgs, int64_t eval_size); // do python udf evaluation
  int modify_desirable(timeval &start, timeval &end, int64_t eval_size);
  int reset_input_store() { return input_store_.reset(); }

  // getter
  int get_desirable() { return desirable_; }
  int get_store_size() { return input_store_.get_saved_size(); }
  int get_result_size() { return result_size_; }


private:
  //common::ObFIFOAllocator alloc_; // input store allocator need to free
  common::ObArenaAllocator alloc_;
  ObExpr *expr_; // python_udf_expr
  ObPUInputStore input_store_; // 不同UDF间使用同一列存在冗余缓存, 公共表达式部份本来也存在冗余

  // 运行时参数... 
  int64_t batch_size_; // 系统参数，关系到存取时最大空间
  int64_t desirable_; // 理想的运行时计算size

  // 运算结果暂存
  int result_size_;
  void *result_store_;
  std::shared_ptr<arrow::Table> result_arrow_store_;
};
typedef common::ObDList<ObPythonUDFCell> PythonUDFCellList;


class ObPUStoreController
{
public:
  ObPUStoreController() : 
    cells_list_(),
    other_store_(),
    capacity_(DEFAULT_PU_LENGTH),
    desirable_(0),
    stored_input_cnt_(0),
    stored_output_cnt_(0),
    output_idx_(0),
    batch_size_(256),
    tenant_id_(500)
  {} 
  ~ObPUStoreController() { free(); }
  int init(int64_t batch_size,
           const common::ObIArray<ObExpr *> &udf_exprs, 
           const common::ObIArray<ObExpr *> &input_exprs,
           const uint64_t tenant_id);
  int free();
  int store(ObEvalCtx &eval_ctx, ObBatchRows &brs);
  int process();
  int restore(ObEvalCtx &eval_ctx, ObBatchRows &brs, int64_t output_size);
  int resize(int64_t size); // 扩容
  int get_desirable() { // 根据各cell的predict size确定desirable的大小
    ObPythonUDFCell* header = cells_list_.get_header();
    for (ObPythonUDFCell* cell = cells_list_.get_first(); 
        cell != header; 
        cell = cell->get_next()) {
      desirable_ = max(desirable_, cell->get_desirable());
    }
    return desirable_;
  }; 
  bool is_full() { return stored_input_cnt_ > desirable_ * (BUFFER_MAG - 1); }
  bool is_empty() { return stored_input_cnt_ == 0; }
  bool can_output() { return output_idx_ < stored_output_cnt_; }
  bool end_output() { return output_idx_ == stored_output_cnt_; }

private:
  common::ObArenaAllocator alloc_; // alloc cells
  PythonUDFCellList cells_list_;
  ObColInputStore other_store_;

  // 运算过程控制参数
  int64_t capacity_; // 缓存容量
  int64_t desirable_; // 理想的predict size
  int64_t stored_input_cnt_;
  int64_t stored_output_cnt_;
  int64_t output_idx_;

  int64_t batch_size_; // 系统参数，关系到存取时最大空间

  uint64_t tenant_id_; // 租户id
};

class ObPythonUDFSpec : public ObOpSpec
{
public:
  OB_UNIS_VERSION_V(1);
public:
  ObPythonUDFSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  ~ObPythonUDFSpec();
  
  //void* _save; //for Python Interpreter Thread State
  ExprFixedArray udf_exprs_; // all python udf rt exprs
  ExprFixedArray input_exprs_; // other input rt exprs(not calculated by python udfs)
};

class ObPythonUDFOp : public ObOperator
{
public:
  ObPythonUDFOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  ~ObPythonUDFOp();

  static int init_udfs(const common::ObIArray<ObExpr *> &udf_exprs);
  static int import_udf(const share::schema::ObPythonUDFMeta &udf_meta);

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;

private:
  int find_predict_size(ObExpr *expr, int32_t &predict_size);
  int clear_calc_exprs_evaluated_flags();

private:
  int predict_size_; //每次python udf计算的元组数
  ObPUStoreController controller_;

  void* _save; //for Python Interpreter Thread State
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SUBQUERY_OB_PYTHON_UDF_OP_H_