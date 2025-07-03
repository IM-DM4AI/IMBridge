#define USING_LOG_PREFIX SQL_ENG

#include "ob_python_udf_op.h"
#include "ob_udf_scheduler.h"
#include "ob_shared_memory_manager.h"
#include "ob_arrow_transform_util.h"
#include "ob_async_state_slots.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include <mutex>

namespace oceanbase
{
using namespace common;
namespace sql
{

static bool with_batch_control_ = false; // 是否进行batch size控制

static bool with_async_execute = false; // Whether use async execute

static bool output_first = true; // 异步任务是否优先处理输出

OB_SERIALIZE_MEMBER((ObPythonUDFSpec, ObOpSpec),
                    udf_exprs_,
                    input_exprs_);

ObPythonUDFSpec::ObPythonUDFSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type), udf_exprs_(alloc), input_exprs_(alloc) {}

ObPythonUDFSpec::~ObPythonUDFSpec() {}

ObPythonUDFOp::ObPythonUDFOp(
    ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input), controller_(), controller_lists(3)
{
  int ret = OB_SUCCESS;
  for(int i = 0;i<3;i++){
    ctl_queue.push(i);
  }
  //predict_size_ = 256;
  //predict_size_ = MY_SPEC.max_batch_size_; //default
  const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  //ObMemAttr attr(tenant_id, ObModIds::RESTORE, ObCtxIds::MEMSTORE_CTX_ID);
  //controller_.set_attr(attr);
  /*if (OB_FAIL(controller_.init(MY_SPEC.max_batch_size_,
                               MY_SPEC.udf_exprs_, 
                               MY_SPEC.input_exprs_,
                               tenant_id))) {
    ret = OB_INIT_FAIL;
    LOG_WARN("Init python udf store controller failed", K(ret));
  }*/
}

ObPythonUDFOp::~ObPythonUDFOp() {
  //controller_.free();
}

int ObPythonUDFOp::inner_open()
{
  int ret = OB_SUCCESS;

  //initialize Python Intepreter
  //Py_InitializeEx(!Py_IsInitialized());
  //_save = PyEval_SaveThread();

  // init context
  const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  // create async slots
  // if(with_async_execute){
  //   auto &slots = AsyncStateSlots::GetOrCreateInstance(IMLaneScheduler::GetOrCreateInstance()->sys_cpu_core_nums);
  //   if(slots != nullptr && OB_FAIL(slots->InitControllerSlots(MY_SPEC.max_batch_size_,
  //                                                                                 MY_SPEC.udf_exprs_, 
  //                                                                                 MY_SPEC.input_exprs_,
  //                                                                                 tenant_id))){
  //     ret = OB_INIT_FAIL;
  //     LOG_WARN("Init controller slots failed", K(ret));                                          
  //   }
  // }
  for(auto &ctl : controller_lists){
    ctl.init(MY_SPEC.max_batch_size_,
            MY_SPEC.udf_exprs_, 
            MY_SPEC.input_exprs_,
            tenant_id);
  }
  if(OB_SUCC(ret) && OB_FAIL(controller_.init(MY_SPEC.max_batch_size_,
                               MY_SPEC.udf_exprs_, 
                               MY_SPEC.input_exprs_,
                               tenant_id))) {
    ret = OB_INIT_FAIL;
    LOG_WARN("Init python udf store controller failed", K(ret));
  //} else if (OB_FAIL(init_udfs(MY_SPEC.udf_exprs_))) {
  //  ret = OB_INIT_FAIL;
  //  LOG_WARN("Import python udfs", K(ret));
  } else {
    // check attrs
    ret = ObOperator::inner_open();
  }
  //ret = ObOperator::inner_open();
  return ret;
}

int ObPythonUDFOp::inner_close()
{
  int ret = OB_SUCCESS;

  //PyEval_RestoreThread((PyThreadState *)_save);
  //Py_FinalizeEx(); // Python Intepreter
  if(with_async_execute){
    AsyncStateSlots::ResetInstance();
  }

  // free attrs;
  if (OB_FAIL(controller_.free())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Free python udf store controller failed", K(ret));
  } else {
    ret = ObOperator::inner_close();
  }
  //ret = ObOperator::inner_close();
  /*PyObject *dycacher; // dycacher module
  PyObject *clearFunc; // reset function
  if((dycacher = PyImport_ImportModule("dycacher")) == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get dycacher module", K(ret));
  } else if ((clearFunc = PyObject_GetAttrString(dycacher, "reset_cache")) == nullptr ||
              !PyCallable_Check(clearFunc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get reset cacher function", K(ret));
  } else if (PyObject_CallObject(clearFunc, NULL) == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to call reset cacher function", K(ret));
  } else {}*/
  return ret;
}

int ObPythonUDFOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  // reset attrs
  const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  if (OB_FAIL(controller_.free()) || 
      OB_FAIL(controller_.init(MY_SPEC.max_batch_size_,
                               MY_SPEC.udf_exprs_, 
                               MY_SPEC.input_exprs_,
                               tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = ObOperator::inner_rescan();
  }
  //ret = ObOperator::inner_rescan();
  return ret;
}

void ObPythonUDFOp::destroy()
{
  ObOperator::destroy();
}

int ObPythonUDFOp::inner_get_next_row() 
{
  int ret = OB_SUCCESS;
  // not support yet
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get row from child failed", K(ret));
    }
  } else {/* do nothing */}
  return ret;
}

/*使用缓存，确保每轮迭代能够以合适的批次大小计算Python UDF
* 1. 调用Python UDF expr的子expr
* 2. 复制frame上的输入至input缓存
* 3. 重复1，2至input缓存到达阈值
* 4. 直接在input缓存上计算Python UDF，并将结果保存至output缓存
* 5. 将output缓存复制回frame上的输出
* 6. 重复5至output缓存清空
 */
int ObPythonUDFOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  // get data from buffer
  /*
  // Ensure GIL
  bool nStatus = PyGILState_Check();
  PyGILState_STATE gstate;
  if(!nStatus) {
    gstate = PyGILState_Ensure();
    nStatus = true;
  }
  */
  //struct timeval t1, t2;
  //gettimeofday(&t1, NULL);
  
  if(with_async_execute){// async execute
    clear_evaluated_flag();
    const ObBatchRows *child_brs = nullptr;
    // auto &slots = AsyncStateSlots::GetOrCreateInstance();
    auto &scheduler = IMLaneScheduler::GetOrCreateInstance(true);
    bool no_data_to_return = true;
    while (OB_SUCC(ret) && no_data_to_return)
    {
      if(output_first){
      // output first
      // real no data to return
      if(brs_.end_ && res_collect.empty()){
        return ret;
      }
      if(res_collect.size()){
        // LOG_INFO("[Clien] Handle output.", K(ret));
        int future_id = -1;
        for(auto it = res_collect.begin() ;it != res_collect.end();it++){
          if((*it)->wait_for(std::chrono::seconds(0)) == std::future_status::ready){
            future_id = (*it)->get();
            res_collect.erase(it);
            // LOG_INFO("[Clien] Get future id.", K(ret), K(future_id));
            break;
          }
        }
        if (future_id != -1){
          auto &controller_slot = controller_lists[future_id];
          // LOG_INFO("[Clien] store output.", K(ret));
          if (OB_FAIL(controller_slot.restore(eval_ctx_, brs_, max_row_cnt)))
          {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("Restore output batchrows failed.", K(ret));
          }
          else
          {
            // reset all spec filters
            FOREACH_CNT_X(e, MY_SPEC.filters_, OB_SUCC(ret))
            {
              (*e)->clear_evaluated_flag(eval_ctx_);
              (*e)->get_eval_info(eval_ctx_).clear_evaluated_flag();
            }
          }
          if (OB_SUCC(ret))
          {
            // LOG_INFO("[Clien] add id to queue.", K(ret), K(future_id));
            ctl_queue.push(future_id);
            no_data_to_return = false;
          }
        }
      }
      if(no_data_to_return && !brs_.end_){
        if (slots_id == -1 && ctl_queue.size())
        {
          // int futrue_size = res_collect.size();
          // int working_worker = scheduler->working_threads_num.load();
          // int queue_size = slots->queue_size.load();
          slots_id = ctl_queue.front();
          ctl_queue.pop();
          // LOG_INFO("[Clien] get a slot id.", K(ret), K(slots_id));
        }
        if(slots_id != -1){
          int id = slots_id;
          auto &controller_slot = controller_lists[id];
          if (!save_input && !brs_.end_){
            // LOG_INFO("[Clien] pull input.", K(ret));
            if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs)))
            {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("Get child next batch failed.", K(ret));
            }
            else if (brs_.copy(child_brs))
            {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("Copy child batch rows failed.", K(ret));
            }
            if(!brs_.end_){
              // LOG_INFO("[Clien] store input.", K(ret));
              if(OB_SUCC(controller_slot.store(eval_ctx_, brs_))){
                save_input = true;
              }else{
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("failed to save input to slots.", K(ret));
              }
            }else{
              ctl_queue.push(slots_id);
              slots_id = -1;
              id = -1;
            }
          }
          if (OB_SUCC(ret) && id != -1){
            // LOG_INFO("[Clien] create async.", K(ret));
            std::unique_ptr<std::future<int>> async_res = scheduler->async_scheduler->enqueue([&controller_slot, id]()
                                                                                            { return controller_slot.async_process(id); });
            if (async_res != nullptr && async_res->valid())
            {
              // LOG_INFO("[Clien] save async.", K(ret));
              res_collect.push_back(std::move(async_res));
              slots_id = -1;
              save_input = false;
            }
          }
        }
      }
      }else{
      // input first
      // slots_id == -1 means: current operator is not using async slots to store data
      // slots_id != -1 means: have used slots but no create async task、
      // 1. 确定是否有slots并且有数据可以拉取, 有就走创建任务，没有就走处理输出
      if (slots_id == -1 && !brs_.end_ &&  ctl_queue.size()) 
      {
        // int futrue_size = res_collect.size();
        // int working_worker = scheduler->working_threads_num.load();
        // int queue_size = slots->queue_size.load();
        slots_id = ctl_queue.front();
        ctl_queue.pop();
         // LOG_INFO("[Clien] get a slot id.", K(ret), K(slots_id), K(futrue_size), K(working_worker), K(queue_size));
      }
      if (slots_id == -1)
      { // no slots, handle output
        if (res_collect.empty())
        {
          return ret;
        }
        // LOG_INFO("[Clien] Handle output.", K(ret));
        int future_id = -1;
        for(auto it = res_collect.begin() ;it != res_collect.end();it++){
          if((*it)->wait_for(std::chrono::seconds(0)) == std::future_status::ready){
            future_id = (*it)->get();
            res_collect.erase(it);
            // LOG_INFO("[Clien] Get future id.", K(ret), K(future_id));
            break;
          }
        }
        if (future_id != -1)
        {
          try
          {
            auto &controller_slot = controller_lists[future_id];
            if (OB_FAIL(controller_slot.restore(eval_ctx_, brs_, max_row_cnt)))
            {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("Restore output batchrows failed.", K(ret));
            }
            else
            {
              // reset all spec filters
              FOREACH_CNT_X(e, MY_SPEC.filters_, OB_SUCC(ret))
              {
                (*e)->clear_evaluated_flag(eval_ctx_);
                (*e)->get_eval_info(eval_ctx_).clear_evaluated_flag();
              }
            }
            if (OB_SUCC(ret))
            {
              // LOG_INFO("[Clien] add id to queue.", K(ret), K(future_id));
              ctl_queue.push(future_id); // add id to queue
              no_data_to_return = false;
            }
          }
          catch (const std::exception &e)
          {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Process async python udf collect failed.", K(ret));
            break;
          }
        }
      }
      else
      { // has slots, handle input
        int id = slots_id;
        auto &controller_slot = controller_lists[id];
        if (!save_input && !brs_.end_)
        {
          if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs)))
          {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("Get child next batch failed.", K(ret));
          }
          else if (brs_.copy(child_brs))
          {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("Copy child batch rows failed.", K(ret));
          }
          if(!brs_.end_){
            // LOG_INFO("[Clien] store input.", K(ret));
            if(OB_SUCC(controller_slot.store(eval_ctx_, brs_))){
              save_input = true;
            }else{
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("failed to save input to slots.", K(ret));
            }
          }else{
            ctl_queue.push(slots_id);
            slots_id = -1;
            id = -1;
          }
        }
        if (OB_SUCC(ret) && id != -1)
        {
          // LOG_INFO("[Clien] create async.", K(ret));
          std::unique_ptr<std::future<int>> async_res = scheduler->async_scheduler->enqueue([&controller_slot, id]()
                                                                                            { return controller_slot.async_process(id); });
          if (async_res != nullptr && async_res->valid())
          {
            // LOG_INFO("[Clien] save async.", K(ret));
            slots_id = -1;
            save_input = false;
            res_collect.push_back(std::move(async_res));
          }
        }
      }
    }
    }
  }else{
    if (with_batch_control_) {
      if (OB_SUCC(ret) && !controller_.can_output()) {
        clear_evaluated_flag();
        controller_.resize(controller_.get_desirable() * BUFFER_MAG);
        const ObBatchRows *child_brs = nullptr;
        while (OB_SUCC(ret) && !brs_.end_ && !controller_.is_full()) { // while loop直至缓存填满
          if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Get child next batch failed.", K(ret));
          } else if (brs_.copy(child_brs)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Copy child batch rows failed.", K(ret));
          } else if (OB_FAIL(controller_.store(eval_ctx_, brs_))){
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Save input batchrows failed.", K(ret));
          }
        }
        if (OB_FAIL(ret) || OB_FAIL(controller_.process())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Process python udf failed.", K(ret));
        }
      }
    } else { // 无batch size控制
      if (OB_SUCC(ret)) { 
        clear_evaluated_flag();
        const ObBatchRows *child_brs = nullptr;
        /*
        if(OB_FAIL(controller_.resize(controller_.get_desirable() * BUFFER_MAG))){
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Do Python UDF Cell resize failed.", K(ret));
        }
        else */ // schedule execute
        if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Get child next batch failed.", K(ret));
        } else if (brs_.copy(child_brs)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Copy child batch rows failed.", K(ret));
        } else if (OB_FAIL(controller_.store(eval_ctx_, brs_))){
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Save input batchrows failed.", K(ret));
        } else if (OB_FAIL(controller_.process())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Process python udf failed.", K(ret));
        } else {}
      }
    }

    if (OB_FAIL(ret) || OB_FAIL(controller_.restore(eval_ctx_, brs_, max_row_cnt))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Restore output batchrows failed.", K(ret));
    } else {
      // reset all spec filters
      FOREACH_CNT_X(e, MY_SPEC.filters_, OB_SUCC(ret)) {
        (*e)->clear_evaluated_flag(eval_ctx_);
        (*e)->get_eval_info(eval_ctx_).clear_evaluated_flag();
      }
    }
  }
  /*gettimeofday(&t2, NULL);
  double timeuse = (t2.tv_sec - t1.tv_sec) * 1000000 + (double)(t2.tv_usec - t1.tv_usec);
  double time_s = timeuse / 1000;
  std::fstream time_log;
  // operater time log
  time_log.open("/home/test/experiments/oceanbase/revision/limit1/op_time.log", std::ios::app);
  time_log << "operator time: " << time_s << std::endl;
  time_log.close(); */

  //PyGC_Enable();
  //PyGC_Collect();
  // Release GIL
  // if(nStatus) {
  //   PyGILState_Release(gstate);
  // }

  /*std::ofstream outputFile("/home/test/experiments/oceanbase/px/log/batch_size.txt", std::ios::app);
  if (outputFile) {
    outputFile << brs_.size_ << std::endl;
  }
  outputFile.close();*/

  return ret;
}

/* find predict_size of buffer */
int ObPythonUDFOp::find_predict_size(ObExpr *expr, int32_t &predict_size)
{
  int ret = OB_SUCCESS;
  /*int expr_size = 256; //default
  if (expr->type_ == T_FUN_PYTHON_UDF) {
    int size = static_cast<ObPythonUdfInfo *>(expr->extra_info_)->predict_size;
    expr_size = size > expr_size ? size : expr_size;
  } else {
    for (int32_t i = 0; i < expr->arg_cnt_; i++) {
      find_predict_size(expr->args_[i], expr_size);
    }
  }
  if (predict_size < expr_size)
    predict_size = expr_size;
  if (predict_size > max_buffer_size_)
    predict_size = max_buffer_size_;*/
  return ret;
}

int ObPythonUDFOp::clear_calc_exprs_evaluated_flags() 
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < MY_SPEC.calc_exprs_.count(); i++) {
    MY_SPEC.calc_exprs_[i]->get_eval_info(eval_ctx_).clear_evaluated_flag();
  }
  return ret;
}

int ObPythonUDFOp::init_udfs(const common::ObIArray<ObExpr *> &udf_exprs)
{
  int ret = OB_SUCCESS;
  //Acquire GIL
  // bool nStatus = PyGILState_Check();
  // PyGILState_STATE gstate;
  // if(!nStatus) {
  //   gstate = PyGILState_Ensure();
  //   nStatus = true;
  // }
  // init all python udfs
  for (int i = 0; OB_SUCC(ret) && i < udf_exprs.count(); ++i) {
    ObExpr *udf_expr = udf_exprs.at(i);
    ObPythonUDFMeta udf_meta = static_cast<ObPythonUdfInfo *>(udf_expr->extra_info_)->udf_meta_;
    if (udf_meta.init_) {
      LOG_DEBUG("udf meta has already inited", K(ret));
    } else if (udf_meta.ret_ == ObPythonUDF::PyUdfRetType::UDF_UNINITIAL) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("udf meta ret type is null", K(ret));
    } else if (udf_meta.pycall_ == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("udf meta pycall is null", K(ret));
    } else {
      // check udf param types
      for (int j = 0; OB_SUCC(ret) && j < udf_expr->arg_cnt_; ++j) {
        ObExpr *expr = udf_expr->args_[j];
        if (expr == nullptr) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("python udf arg expr is null", K(ret));
        } else {
          switch(expr->datum_meta_.type_) {
            case ObCharType :
            case ObVarcharType :
            case ObTinyTextType :
            case ObTextType :
            case ObMediumTextType :
            case ObLongTextType : 
              if(udf_meta.udf_attributes_types_.at(j) != ObPythonUDF::STRING) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("the type of param is incorrect", K(ret), K(j));
              }
              break;
            case ObTinyIntType :
            case ObSmallIntType :
            case ObMediumIntType :
            case ObInt32Type :
            case ObIntType : 
              if(udf_meta.udf_attributes_types_.at(j) != ObPythonUDF::INTEGER) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("the type of param is incorrect", K(ret), K(j));
              }
              break;
            case ObDoubleType : 
              if(udf_meta.udf_attributes_types_.at(j) != ObPythonUDF::REAL) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("the type of param is incorrect", K(ret), K(j));
              }
              break;
            case ObNumberType : 
                //decimal
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("not support decimal", K(ret), K(j));
                break;
            default : 
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("not support param type", K(ret));
          }
        }
      }
      // import python udf code
      if (OB_FAIL(ret) || OB_FAIL(import_udf(udf_meta))) {
        ret = OB_INIT_FAIL;
        LOG_WARN("Fail to import udf", K(ret));
      } else {
        udf_meta.init_ = true;
      }
    }
  }
  //release GIL
  // if(nStatus)
  //   PyGILState_Release(gstate);
  return ret;
}

int ObPythonUDFOp::import_udf(const share::schema::ObPythonUDFMeta &udf_meta)
{
  int ret = OB_SUCCESS;

  //runtime variables
  PyObject *pModule = NULL;
  PyObject *dic = NULL;
  PyObject *v = NULL;
  PyObject *pInitial = NULL;

  //name
  std::string name(udf_meta.name_.ptr());
  name = name.substr(0, udf_meta.name_.length());
  std::string pyinitial_handler = name + "_pyinitial";
  std::string pyfun_handler = name + "_pyfun";
  //pycall
  std::string pycall(udf_meta.pycall_.ptr());
  pycall = pycall.substr(0, udf_meta.pycall_.length());
  pycall.replace(pycall.find("pyinitial"), 9, pyinitial_handler);
  pycall.replace(pycall.find("pyfun"), 5, pyfun_handler);
  const char* pycall_c = pycall.c_str();

  // prepare and import python code
  pModule = PyImport_AddModule("__main__"); // load main module
  if(pModule == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to import main module", K(ret));
  } else if ((dic = PyModule_GetDict(pModule)) == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get main module dic", K(ret));
  } else if ((v = PyRun_StringFlags(pycall_c, Py_file_input, dic, dic, NULL)) == nullptr) {
    ObExprPythonUdf::process_python_exception();
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to write pycall into module", K(ret));
  } else if ((pInitial = PyObject_GetAttrString(pModule, pyinitial_handler.c_str())) == nullptr) {
    ObExprPythonUdf::process_python_exception();
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to import pyinitial", K(ret));
  } else if (PyObject_CallObject(pInitial, NULL) == nullptr) {
    ObExprPythonUdf::process_python_exception();
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to run pyinitial", K(ret));
  } else {
    LOG_DEBUG("Import python udf handler", K(ret));
  }
  return ret;
}

/* ---------------------------------- ObColInputStore -------------------------------------*/
int ObColInputStore::init(const common::ObIArray<ObExpr *> &exprs,
                          int64_t batch_size,
                          int64_t length)
{
  int ret = OB_SUCCESS;
  exprs_.init(exprs.count());
  datums_copy_.init(exprs.count());
  for (int i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObExpr *expr = exprs.at(i);
    ObDatum *datums_buf = nullptr;
    datums_buf = static_cast<ObDatum *>(buf_alloc_.alloc(sizeof(ObDatum) * length));
    if (datums_buf == nullptr) {
      //分配datums_buf空间
      ret = OB_INIT_FAIL;
      LOG_WARN("Memory allocation failed.", K(ret));
    } else if (OB_FAIL(exprs_.push_back(expr)) || OB_FAIL(datums_copy_.push_back(datums_buf))) {
      ret = OB_INIT_FAIL;
      LOG_WARN("Init datums_copy and other exprs failed.", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    length_ = length;
    batch_size_ = batch_size;
    saved_size_ = 0;
    output_idx_ = 0;
    inited_ = true;
  }
  return ret;
}

int ObColInputStore::free()
{
  int ret = OB_SUCCESS;
  tmp_alloc_.reset();
  /*for (int i = 0; i < datums_copy_.count(); ++i) {
    ObDatum *datums_buf = datums_copy_.at(i);
    buf_alloc_.free(datums_buf);
  }*/
  return ret;
}

int ObColInputStore::reuse()
{
  int ret = OB_SUCCESS;
  tmp_alloc_.reset();
  saved_size_ = 0;
  output_idx_ = 0;
  return ret;
}

int ObColInputStore::reset(int64_t length)
{
  int ret = OB_SUCCESS;
  if (length <= length_) {
    reuse();
  } else if (OB_FAIL(free())) {
    // memory leak
    ret = OB_ERR_UNEXPECTED;
  } else {
    datums_copy_.reset();
    datums_copy_.init(exprs_.count());
    for (int i = 0; i < exprs_.count(); ++i) {
      ObExpr *e = exprs_.at(i);
      ObDatum *datums_buf = static_cast<ObDatum *>(buf_alloc_.alloc(length * sizeof(ObDatum)));
      datums_copy_.push_back(datums_buf);
    }
    length_ = length;
    saved_size_ = 0;
    output_idx_ = 0;
  }
  return ret;
}

int ObColInputStore::save_batch(ObEvalCtx &eval_ctx, ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  int cnt = brs.size_ - brs.skip_->accumulate_bit_cnt(brs.size_);
  if (saved_size_ + cnt > length_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Other Input Store overflow.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_.count(); ++i) {
      ObExpr *expr = exprs_.at(i);
      expr->cast_to_uniform(brs.size_, eval_ctx); // 数据格式转换过程可能会产生额外开销
      ObDatum *src = expr->locate_batch_datums(eval_ctx);
      ObDatum *dst = datums_copy_.at(i);
      int j = 0, zero = 0;
      int *src_idx;
      if (expr->is_const_expr())
        src_idx = &zero;
      else
        src_idx = &j;
      int dst_idx = saved_size_; // use dst idx to reform data
      for (j = 0; OB_SUCC(ret) && j < brs.size_; ++j) {
        if (brs.skip_->at(j)) {
          /* do nothing */
        } else {
          dst[dst_idx++].deep_copy(src[*src_idx], tmp_alloc_);
        }
      }
    }
    if (OB_SUCC(ret)) {
      saved_size_ += cnt;
    }
  }
  return ret;
}

int ObColInputStore::save_vector(ObEvalCtx &eval_ctx, ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  int cnt = brs.size_ - brs.skip_->accumulate_bit_cnt(brs.size_);
  if (saved_size_ + cnt > length_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Other Input Store overflow.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_.count(); ++i) {
      ObExpr *expr = exprs_.at(i);
      ObIVector *vector = expr->get_vector(eval_ctx);
      ObDatum *dst = datums_copy_.at(i);
      int dst_idx = saved_size_; // use dst idx to reform data
      for (int64_t j = 0; OB_SUCC(ret) && j < brs.size_; ++j) {
        if (brs.skip_->at(j)) {
          /* do nothing */
        } else {
          // deep copy from vector to datums
          ObLength copy_len = 0;
          const char *payload;
          vector->get_payload(j, payload, copy_len);
          char *buf = nullptr;
          if (copy_len == 0) {
            dst[dst_idx].ptr_ = nullptr;
            dst[dst_idx].len_ = 0;
            ++dst_idx;
          } else {
            buf = static_cast<char *>(tmp_alloc_.alloc(copy_len));
            if (buf == nullptr) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(copy_len), K(ret));
            } else {
              MEMCPY(buf, payload, copy_len);
              dst[dst_idx].ptr_ = buf;
              dst[dst_idx].len_ = copy_len;
              ++dst_idx;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      saved_size_ += cnt;
    }
  }
  return ret;
}

int ObColInputStore::load_batch(ObEvalCtx &eval_ctx, int64_t load_size)
{
  int ret = OB_SUCCESS;
  if (output_idx_ + load_size > saved_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not enough data to load.", K(ret));
  } else {
    for (int64_t i = 0; i < exprs_.count(); ++i) {
      ObExpr *expr = exprs_.at(i);
      ObDatum *src = datums_copy_.at(i);
      ObDatum *dst = expr->locate_batch_datums(eval_ctx);
      for (int j = 0; j < load_size; ++j) {
        dst[j].set_datum(src[output_idx_ + j]);
      }
    }
    if (OB_SUCC(ret)) {
      output_idx_ += load_size;
    }
  }
  return ret;
}

int ObColInputStore::load_vector(ObEvalCtx &eval_ctx, int64_t load_size)
{
  int ret = OB_SUCCESS;
  if (output_idx_ + load_size > saved_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not enough data to load.", K(ret));
  } else {
    for (int64_t i = 0; i < exprs_.count(); ++i) {
      ObExpr *expr = exprs_.at(i);
      //if (expr->datum_meta_.type_ == ObVarcharType || expr->datum_meta_.type_ == ObCharType)
        //expr->init_vector(eval_ctx, VEC_DISCRETE, load_size, false);
      //else
      expr->init_vector(eval_ctx, expr->get_default_res_format(), load_size, true);
      ObDatum *src = datums_copy_.at(i);
      ObIVector *dst = expr->get_vector(eval_ctx);
      for (int j = 0; j < load_size; ++j) {
        dst->set_payload(j, src[output_idx_ + j].ptr_, src[output_idx_ + j].len_);
      }
    }
    if (OB_SUCC(ret)) {
      output_idx_ += load_size;
    }
  }
  return ret;
}
/* ---------------------------------- ObPUInputStore -------------------------------- */
int ObPUInputStore::init(common::ObIAllocator *alloc, ObExpr *expr, int64_t length)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Repeat allocation", K(ret));
  } else if (alloc == nullptr || expr == nullptr || length <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("Uninit allocator and expression.", K(ret));
  } else if (expr->type_ != T_FUN_PYTHON_UDF) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Not Python UDF Expr Type.", K(ret));
  } else {
    buf_alloc_ = alloc;
    expr_ = expr;
    if (OB_FAIL(alloc_data_ptrs(length)) || data_ptrs_ == nullptr) {
      ret = OB_INIT_FAIL;
      LOG_WARN("Init ObPUInputStore failed.", K(ret), K(alloc), K(expr->arg_cnt_));
    } else {
      inited_ = true;
      saved_size_ = 0;
    }
  }
  return ret;
}

int ObPUInputStore::alloc_data_ptrs(int64_t length)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_) ) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Repeat allocation.", K(ret));
  } else if (buf_alloc_ == nullptr || expr_ == nullptr || expr_->arg_cnt_ <= 0 || length <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("Uninit allocator and expression.", K(ret));
  } else {
    // allocated by buf allocator
    data_ptrs_ = static_cast<char **>(buf_alloc_->alloc(expr_->arg_cnt_ * sizeof(char *))); // data lists
    if (data_ptrs_ == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Allocate data_ptr_ memory failed", K(ret), K(buf_alloc_), K(expr_->arg_cnt_));
    }
    for (int i = 0; i < expr_->arg_cnt_ && OB_SUCC(ret); ++i) {
      ObExpr *e = expr_->args_[i];
      data_ptrs_[i] = nullptr;
      /* allocate by datum type */
      switch(e->datum_meta_.type_) {
        case ObCharType:
        case ObVarcharType:
        case ObTinyTextType:
        case ObTextType:
        case ObMediumTextType:
        case ObLongTextType: {
          data_ptrs_[i] = reinterpret_cast<char *>(buf_alloc_->alloc(length * sizeof(ObDatum))); // string lists
          break;
        }
        case ObTinyIntType:
        case ObSmallIntType:
        case ObMediumIntType:
        case ObInt32Type:
        case ObIntType: {
          data_ptrs_[i] = reinterpret_cast<char *>(buf_alloc_->alloc(length * sizeof(int))); // int lists
          break;
        }
        case ObDoubleType: {
          data_ptrs_[i] = reinterpret_cast<char *>(buf_alloc_->alloc(length * sizeof(double))); // double lists
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("Unsupported input arg type, alloc_data_ptrs failed.", K(ret));
        }
      }
      if (data_ptrs_[i] == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Allocate data_ptr_[i] memory failed.", K(ret), K(buf_alloc_), K(length), K(i), K(e->datum_meta_.type_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    length_ = length;
  }
  return ret;
}

int ObPUInputStore::reuse()
{
  int ret = OB_SUCCESS;
  tmp_alloc_.reset();
  saved_size_ = 0;
  return ret;
}

int ObPUInputStore::reset(int64_t length /* default = 0 */)
{
  int ret = OB_SUCCESS;
  if(length <= length_) {
    ret = reuse();
  } else if (OB_FAIL(free())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(alloc_data_ptrs(length))) {
    ret = OB_INIT_FAIL;
  } else {
    saved_size_ = 0;
    inited_ = true;
  }
  return ret;
}

int ObPUInputStore::free() {
  int ret = OB_SUCCESS;
  length_ = 0;
  //data_ptrs_ = nullptr;
  saved_size_ = 0;
  inited_ = false;
  tmp_alloc_.reset();
  return ret;
}

int ObPUInputStore::save_batch(ObEvalCtx &eval_ctx, ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  int brs_cnt = brs.size_ - brs.skip_->accumulate_bit_cnt(brs.size_);
  if (expr_ == nullptr || expr_->arg_cnt_ <= 0) {
    // empty exprs_: do nothing
  } else if (data_ptrs_ == nullptr || !inited_) {
    ret = OB_NOT_INIT;
  } else if (saved_size_ + brs_cnt <= length_) {
    for (int64_t i = 0; i < expr_->arg_cnt_; i++) {
      ObExpr *e = expr_->args_[i];
      ObDatum *src = e->locate_batch_datums(eval_ctx);
      int j = 0, zero = 0;
      int64_t dst_idx = saved_size_;
      int *src_idx;
      if (!expr_->args_[i]->is_const_expr()) 
        src_idx = &j;
      else 
        src_idx = &zero;
      switch(e->datum_meta_.type_) {
        case ObCharType:
        case ObVarcharType:
        case ObTinyTextType:
        case ObTextType:
        case ObMediumTextType:
        case ObLongTextType: {
          ObDatum *dst = reinterpret_cast<ObDatum *>(data_ptrs_[i]);
          for (j = 0; j < brs.size_; j++) {
            if (!brs.skip_->at(j)) {
              //copy str
              ObDatum &src_datum = src[*src_idx];
              dst[dst_idx++].deep_copy(src_datum, tmp_alloc_);
            }
          }
          break;
        }
        case ObTinyIntType:
        case ObSmallIntType:
        case ObMediumIntType:
        case ObInt32Type:
        case ObIntType: {
          int *dst = reinterpret_cast<int *>(data_ptrs_[i]);
          for (j = 0; j < brs.size_; j++) {
            ObDatum &src_datum = src[*src_idx];
            if (!brs.skip_->at(j)) {
              // copy int
              dst[dst_idx++] = src_datum.get_int();
            }
          }
          break;
        }
        case ObDoubleType: {
          double *dst = reinterpret_cast<double *>(data_ptrs_[i]);
          for (j = 0; j < brs.size_; j++) {
            ObDatum &src_datum = src[*src_idx];
            if (!brs.skip_->at(j)) {
              // copy double
              dst[dst_idx++] = src_datum.get_double();
            }
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("Unsupported input arg type, save data failed in ObPUDataStore.", K(ret));
        }
      }
      if (data_ptrs_[i] == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Allocate data_ptr_[i] memory failed.", K(ret), K(tmp_alloc_), K(length_), K(i), K(e->datum_meta_.type_));
      }
    }
    saved_size_ += brs_cnt;
  } else {
    // op逻辑控制其不能超过阈值
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPUDataStore Stack Overflow.", K(ret));
  }
  return ret;
}

int ObPUInputStore::save_vector(ObEvalCtx &eval_ctx, ObBatchRows &brs)
{
  // not implemented yet
  int ret = OB_NOT_SUPPORTED;
  UNUSED(eval_ctx);
  UNUSED(brs);
  return ret;
}

/* ----------------------------------- ObPythonUDFCell --------------------------------- */
int ObPythonUDFCell::init(ObExpr *expr, 
                          int64_t batch_size, 
                          int64_t length,
                          uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  /*const ObMemAttr mem_attr(tenant_id, "PythonUDF");
  // init python udf expr according to its metadata
  if (OB_FAIL(alloc_.init(ObMallocAllocator::get_instance(),
                          OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr))) {
    ret = OB_NOT_INIT;
    LOG_WARN("Fail to init allocator", K(ret));
  } else */if (OB_FAIL(input_store_.init(&alloc_, expr, length))) {
    ret = OB_NOT_INIT;
    LOG_WARN("Init input store failed.", K(ret));
  } else {
    expr_ = expr;
    //desirable_ = expr->extra_info_.predict_size;
    ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
    desirable_ = info->predict_size; // 初始值256
    batch_size_ = batch_size;
  }
  return ret;
}

int ObPythonUDFCell::free()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(input_store_.free())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Free Python UDF cell failed.", K(ret));
  } else {
    result_size_ = 0;
    result_store_ = nullptr; // possibly memory leak
    /*if (result_store_ != nullptr) {
      PyObject *result_store = reinterpret_cast<PyObject *>(result_store_);
      Py_CLEAR(result_store);
      result_store_ = nullptr;
    }*/
  }
  alloc_.reset();
  return ret;
}

int ObPythonUDFCell::do_store(ObEvalCtx &eval_ctx, ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  if (expr_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("Expr in input store is NULL.", K(ret));
  } else {
    // decide save function according to data transfer type
    // 因为要判断skip并进行重整，不能直接MEMCPY，所以用save_batch方法就足够了
    ObBitVector &my_skip = expr_->get_pvt_skip(eval_ctx);
    for (int64_t i = 0; i < expr_->arg_cnt_ && OB_SUCC(ret); i++) {
      ObExpr *e = expr_->args_[i];
      // eval_batch() = eval_vector() + cast_to_uniform()
      //if (OB_FAIL(e->eval_batch(eval_ctx, *brs.skip_, brs.size_))) {
      //if (OB_FAIL(e->eval_batch(eval_ctx, my_skip, batch_size_))) {
      if (OB_FAIL(e->eval_vector(eval_ctx, brs))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Eval Python UDF args' vector/batch result failed.", K(ret));
      } else if (OB_FAIL(e->cast_to_uniform(brs.size_, eval_ctx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Cast to uniform failed.", K(ret));
      }
    }
    // expr参数结果已经被设置为UNIFORM格式
    if (OB_FAIL(ret) || OB_FAIL(input_store_.save_batch(eval_ctx, brs))) { 
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Save Python UDF input batch failed.", K(ret), K(expr_), K(batch_size_));
    }
  }
  return ret;
}

// not used
int ObPythonUDFCell::do_process_all()
{
  int ret = OB_SUCCESS;
  // pre process
  result_store_ = nullptr;
  result_size_ = 0;
  PyObject *pArgs = nullptr;
  int64_t eval_size;
  struct timeval t1, t2;
  //load numpy api
  // _import_array();
  //gettimeofday(&t1, NULL);
  if (OB_FAIL(wrap_input_numpy(pArgs, eval_size))) { // wrap all input
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Wrap Cell Input Store as Python UDF input args failed.", K(ret));
  } else if (OB_FAIL(eval(pArgs, eval_size))) { // evaluation and keep the result
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Eval Python UDF failed.", K(ret));
  } else { /* do nothing */ }
  Py_CLEAR(pArgs);
  //gettimeofday(&t2, NULL);
  /*double timeuse = (t2.tv_sec - t1.tv_sec) * 1000000 + (double)(t2.tv_usec - t1.tv_usec); // usec
  double tps = eval_size * 1000000 / timeuse; // current tuples per sec
  double time_s = timeuse / 1000;
  std::fstream tps_log;
  tps_log.open("/home/test/experiments/oceanbase/revision/pps/unopt_pf_time_pps.log", std::ios::app);
  tps_log << "process time: " << time_s << std::endl;
  tps_log << "batch size: " << eval_size << std::endl;
  tps_log << "prediction processing speed: " << tps << std::endl;
  tps_log.close();*/
  return ret;
}

int ObPythonUDFCell::do_process()
{
  int ret = OB_SUCCESS;
  // pre process
  result_store_ = nullptr;
  result_size_ = 0;
  struct timeval t1, t2;
  int64_t eval_size = 0;
  //load numpy api
  _import_array();
  ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  // bool batch_size_const = info->udf_meta_.batch_size_const_;
  bool batch_size_const = false;
  for (int idx = 0; OB_SUCC(ret) && idx < input_store_.get_saved_size(); idx += eval_size) {
    gettimeofday(&t1, NULL);
    PyObject *pArgs = nullptr;
    if (OB_FAIL(wrap_input_numpy(pArgs, idx, desirable_, eval_size)) || pArgs == nullptr) { // wrap the input
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Wrap Cell Input Store as Python UDF input args failed.", K(ret));
    } else if (OB_FAIL(eval(pArgs, eval_size))) { // evaluation and keep the result
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Eval Python UDF failed.", K(ret));
    } else {
      gettimeofday(&t2, NULL);
      ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
      if (!batch_size_const)
        modify_desirable(t1, t2, eval_size);

      /*double timeuse = (t2.tv_sec - t1.tv_sec) * 1000000 + (double)(t2.tv_usec - t1.tv_usec); // usec
      double time_s = timeuse / 1000;
      double tps = eval_size * 1000000 / timeuse; // current tuples per sec
      std::fstream tps_log;
      tps_log.open("/home/test/experiments/oceanbase/revision/pps/opt_pf_time_pps.log", std::ios::app);
      tps_log << "process time: " << time_s << std::endl;
      tps_log << "batch size: " << eval_size << std::endl;
      tps_log << "prediction processing speed: " << tps << std::endl;
      tps_log.close();*/
      // std::fstream time_log;
      // time_log.open("/home/test/experiments/oceanbase/opt/pf_time.log", std::ios::app);
      // time_log << time_s << " ";
      // time_log.close(); 
      Py_CLEAR(pArgs);
    }
  }
  return ret;
}

int ObPythonUDFCell::do_restore(ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size)
{
  int ret = OB_SUCCESS;
  if (output_idx + output_size > result_size_) { //确保访问结果数组时不会越界
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Result Store Overflow.", K(ret));
  } else if (expr_ == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected nullptr expr_.", K(ret));
  } else if (expr_->enable_rich_format() && OB_FAIL(do_restore_vector(eval_ctx, output_idx, output_size))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unsupported result type.", K(ret));
  } else if (!expr_->enable_rich_format() && OB_FAIL(do_restore_batch(eval_ctx, output_idx, output_size))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unsupported result type.", K(ret));
  } else {
    expr_->set_evaluated_projected(eval_ctx);
  }
  return ret;
}

int ObPythonUDFCell::do_restore_batch(ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size)
{
  int ret = OB_SUCCESS;
  ObDatum *result_datums = expr_->locate_batch_datums(eval_ctx);
  /*
  PyArrayObject *result_store = reinterpret_cast<PyArrayObject *>(result_store_);
  switch(expr_->datum_meta_.type_) {
    case ObCharType:
    case ObVarcharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType: {
      for (int i = 0; i < output_size; ++i) {
        expr_->reset_ptr_in_datum(eval_ctx, i);
        result_datums[i].set_string(common::ObString(PyUnicode_AsUTF8(
          PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + i)))));
      }
      break;
    }
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      for (int i = 0; i < output_size; ++i) {
        expr_->reset_ptr_in_datum(eval_ctx, i);
        result_datums[i].set_int(PyLong_AsLong(
          PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + i))));
      }
      break;
    }
    case ObDoubleType: {
      for (int i = 0; i < output_size; ++i) {
        expr_->reset_ptr_in_datum(eval_ctx, i);
        result_datums[i].set_double(PyFloat_AsDouble(
          PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + i))));
      }
      break;
    }
    default: {
      //error
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unsupported result type.", K(ret));
    }
  }
  */
  if(!convert_arrow_to_ob_data(result_output, eval_ctx, result_datums, expr_, output_idx, output_size)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to conver arrow to ob data", K(ret));
  }
  return ret;
}

int ObPythonUDFCell::do_restore_vector(ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size)
{
  int ret = OB_SUCCESS;
  //if (!expr_->get_eval_info(eval_ctx).evaluated_) {
  VectorFormat format = VEC_INVALID;
  const ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  switch(info->udf_meta_.ret_) {
    case share::schema::ObPythonUDF::PyUdfRetType::STRING:
      format = VEC_DISCRETE;
      //format = VEC_CONTINUOUS;
    break;
    case share::schema::ObPythonUDF::PyUdfRetType::INTEGER:
    case share::schema::ObPythonUDF::PyUdfRetType::REAL:
      format = VEC_FIXED;
    break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unsupported result type.", K(ret));
  }
  expr_->init_vector_for_write(eval_ctx, format, batch_size_);
  ObIVector *vector = expr_->get_vector(eval_ctx);
  /*
  PyArrayObject *result_store = reinterpret_cast<PyArrayObject *>(result_store_);
  // 构造vector并赋回expr_
  switch(expr_->datum_meta_.type_) {
    case ObCharType:
    case ObVarcharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType: {
      for (int i = 0; i < output_size; ++i) {
        vector->set_string(i, common::ObString(PyUnicode_AsUTF8(
          PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + i)))));
      }
      break;
    }
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      for (int i = 0; i < output_size; ++i) {
        vector->set_int(i, PyLong_AsLong(
          PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + i))));
      }
      break;
    }
    case ObDoubleType: {
      for (int i = 0; i < output_size; ++i) {
        vector->set_double(i, PyFloat_AsDouble(
          PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + i))));
      }
      break;
    }
    default: {
      //error
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unsupported result type.", K(ret));
    }
  }
  */
  if(!convert_arrow_to_ob_data(result_output, eval_ctx, vector, expr_, output_idx, output_size)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to conver arrow to ob data", K(ret));
  }
  return ret;
}


int ObPythonUDFCell::do_restore_arrow_to_batch(ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size)
{
  int ret = OB_SUCCESS;
  ObDatum *result_datums = expr_->locate_batch_datums(eval_ctx);
  if(!convert_arrow_to_ob_data(result_output, eval_ctx, result_datums, expr_, output_idx, output_size)){
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unsupported type.", K(ret));
  };
  return ret;
}

int ObPythonUDFCell::do_restore_arrow_to_vector(ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size){
  int ret = OB_SUCCESS;
  //if (!expr_->get_eval_info(eval_ctx).evaluated_) {
  VectorFormat format = VEC_INVALID;
  const ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  switch(info->udf_meta_.ret_) {
    case share::schema::ObPythonUDF::PyUdfRetType::STRING:
      format = VEC_DISCRETE;
      //format = VEC_CONTINUOUS;
    break;
    case share::schema::ObPythonUDF::PyUdfRetType::INTEGER:
    case share::schema::ObPythonUDF::PyUdfRetType::REAL:
      format = VEC_FIXED;
    break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unsupported result type.", K(ret));
  }
  expr_->init_vector_for_write(eval_ctx, format, batch_size_);
  ObIVector *vector = expr_->get_vector(eval_ctx);
  if(!convert_arrow_to_ob_data(result_output, eval_ctx, vector, expr_, output_idx, output_size)){
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unsupported input type.", K(ret));
  }
  return ret;
}

// warp all saved input
int ObPythonUDFCell::wrap_input_numpy(PyObject *&pArgs, int64_t &eval_size)
{
  return wrap_input_numpy(pArgs, 0, input_store_.get_saved_size(), eval_size);
}


// warp [idx, idx + predict_size_]
int ObPythonUDFCell::wrap_input_numpy(PyObject *&pArgs, int64_t idx, int64_t predict_size, int64_t &eval_size)
{
  int ret = OB_SUCCESS;
  // pArgs = PyTuple_New(expr_->arg_cnt_); // malloc hook
  int64_t saved_size = input_store_.get_saved_size();
  eval_size = (idx + predict_size) < saved_size ? predict_size : saved_size - idx;
  // npy_intp elements[1] = {eval_size};
  if (expr_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("Expr in input store is nullptr.", K(ret));
  } else {
    /*
    for (int i = 0; i < expr_->arg_cnt_; ++i) {
      PyObject *numpyarray = nullptr;
      switch (expr_->args_[i]->datum_meta_.type_) {
        case ObCharType:
        case ObVarcharType:
        case ObTinyTextType:
        case ObTextType:
        case ObMediumTextType:
        case ObLongTextType: {
          // numpyarray = PyArray_New(&PyArray_Type, 
          //                         1, 
          //                         elements, 
          //                         NPY_OBJECT, 
          //                         NULL, 
          //                         reinterpret_cast<PyObject **>(input_store_.get_data_ptr_at(i)), 
          //                         eval_size, 
          //                         0, 
          //                         NULL);
          numpyarray = PyArray_New(&PyArray_Type, 1, elements, NPY_OBJECT, NULL, NULL, 0, 0, NULL);
          //PyObject **unicode_strs = reinterpret_cast<PyObject **>(input_store_.get_data_ptr_at(i)) + idx;
          // construct unicode str
          ObDatum *src = reinterpret_cast<ObDatum *>(input_store_.get_data_ptr_at(i)) + idx;
          for (int j = 0; j < eval_size; ++j) {
            PyObject *unicode_str = PyUnicode_FromStringAndSize(src[j].ptr_, src[j].len_);
            //put unicode string pyobject into numpy array
            //PyArray_SETITEM((PyArrayObject *)numpyarray, (char *)PyArray_GETPTR1((PyArrayObject *)numpyarray, j), 
            //  unicode_strs[j]);
            PyArray_SETITEM((PyArrayObject *)numpyarray, 
              (char *)PyArray_GETPTR1((PyArrayObject *)numpyarray, j), unicode_str);
            
          }
          break;
        }
        case ObTinyIntType:
        case ObSmallIntType:
        case ObMediumIntType:
        case ObInt32Type:
        case ObIntType: {
          numpyarray = PyArray_New(&PyArray_Type, 
                                  1, 
                                  elements, 
                                  NPY_INT32, 
                                  NULL, 
                                  reinterpret_cast<int *>(input_store_.get_data_ptr_at(i)) + idx,  
                                  eval_size, 
                                  0, 
                                  NULL);
          break;
        }
        case ObDoubleType: {
          numpyarray = PyArray_New(&PyArray_Type, 
                                  1, 
                                  elements, 
                                  NPY_FLOAT64, 
                                  NULL, 
                                  reinterpret_cast<double *>(input_store_.get_data_ptr_at(i)) + idx, 
                                  eval_size, 
                                  0, 
                                  NULL);
          break;
        }
        default: {
          //error
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("Unsupported input type.", K(ret));
        }
      }
      if(PyTuple_SetItem(pArgs, i, numpyarray) != 0){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Set numpy array arg failed.", K(ret));
      }
    }
    */
    convert_ob_data_to_arrow(result_arrow_store_, input_store_, expr_, idx, eval_size);
  }
  return ret;
}

int ObPythonUDFCell::eval(PyObject *pArgs, int64_t eval_size)
{
  int ret = OB_SUCCESS;
  // evalatuion in python interpreter
  /*
  // extract pyfun handler
  ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  std::string name(info->udf_meta_.name_.ptr());
  name = name.substr(0, info->udf_meta_.name_.length());
  std::string pyfun_handler = name.append("_pyfun");

  PyObject *pModule = nullptr;
  PyObject *pFunc = nullptr;
  PyObject *pResult = nullptr;

  if ((pModule = PyImport_AddModule("__main__")) == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Import main module failed.", K(ret));
  } else if ((pFunc = PyObject_GetAttrString(pModule, pyfun_handler.c_str())) == nullptr 
      || !PyCallable_Check(pFunc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get function handler failed.", K(ret));
  } else if ((pResult = PyObject_CallObject(pFunc, pArgs)) == nullptr) {
    ObExprPythonUdf::process_python_exception();
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Execute Python UDF error.", K(ret));
  } else {
    // numpy array concat
    if (result_store_ == nullptr) {
      result_store_ = reinterpret_cast<void *>(pResult);
    } else {
      PyObject *concat = PyTuple_New(2);
      PyTuple_SetItem(concat, 0, reinterpret_cast<PyObject *>(result_store_));
      PyTuple_SetItem(concat, 1, pResult);
      result_store_ = reinterpret_cast<void *>(PyArray_Concatenate(concat, 0));
    }
    result_size_ += eval_size;
  }
  */
  
  auto &scheduler =  IMLaneScheduler::GetOrCreateInstance(true);
  int shm_lane_id = scheduler->get_id_from_avaliable_queue();
  SharedMemoryManager shm(std::to_string(shm_lane_id), ProcessKind::CLIENT);
  if(!write_arrow_to_shared_memory(result_arrow_store_, shm, INPUT_TABLE)){
    ret = OB_ERR_UNEXPECTED_UNIT_STATUS;
    LOG_WARN("Failed to write arrow table to shared memory", K(ret));
    return ret;
  }
  scheduler->push_cmd_to_task_queue(shm_lane_id, TASK_UDF_INFER);
  shm.client_wait();
  std::shared_ptr<arrow::Table> res_tmp;
  if(!read_arrow_from_shared_memory(res_tmp, shm, OUTPUT_TABLE)){
    ret = OB_ERR_UNEXPECTED_UNIT_STATUS;
    LOG_WARN("Failed to write arrow table to shared memory", K(ret));
  }
  result_output = arrow::Concatenate(res_tmp->column(0)->chunks()).ValueOrDie(); /// 物化，避免shm销毁
  shm.destroy_shared_memory_object<char>(INPUT_TABLE);
  shm.destroy_shared_memory_object<char>(OUTPUT_TABLE);
  shm.server_post();
  scheduler->push_id_to_avaliable_queue(shm_lane_id);
  result_size_ += eval_size;
  return ret;
}

int ObPythonUDFCell::modify_desirable(timeval &start, timeval &end, int64_t eval_size)
{
  int ret = OB_SUCCESS;
  ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  double timeuse = (end.tv_sec - start.tv_sec) * 1000000 + (double)(end.tv_usec - start.tv_usec); // usec
  double tps = eval_size * 1000000 / timeuse; // current tuples per sec
  // std::fstream tps_log;
  // tps_log.open("/home/test/experiments/oceanbase/opt/both_pps.log", std::ios::app);
  // tps_log << "batch size: " << eval_size << std::endl;
  // tps_log << "prediction processing speed: " << tps << std::endl;
  // tps_log.close();
  if (info->tps_s == 0) { // 初始化
    info->tps_s = tps;
    info->predict_size += info->delta; // 尝试调整
  } else if (info->round > info->round_limit || eval_size != info->predict_size) { //超过轮次，停止调整batch size 或 不符合predict size
    // do nothing
  } else if (tps > info->tps_s) { 
    // 提升阈值λ为10% 且 目前计算数量与给定batch size相符，进行调整
    if (tps < (1 + info->lambda) * info->tps_s)
      info->round++;
    info->tps_s = tps;
    info->predict_size += info->delta;
    /*// 采集batch size变化日志
    std::fstream batch_size_control_log;
    batch_size_control_log.open("/home/test/experiments/oceanbase/revision/batch_control_overhead/batch_size_control.log", std::ios::app);
    batch_size_control_log << "eval size:" << eval_size << ", current desirable predict size:" << info->predict_size << std::endl;
    batch_size_control_log.close();*/
  } else { //tps <= info->tps_s
    // 未达到阈值
    info->tps_s = (1 - info->alpha) * info->tps_s + info->alpha * tps; // 平滑系数α
    if (tps > (1 - info->lambda) * info->tps_s)
      info->round++;
  }
  if (OB_SUCC(ret)) {
    desirable_ = info->predict_size;
  }
  return ret;
}

/* ----------------------------------- ObPUStoreController --------------------------------- */
int ObPUStoreController::init(int64_t batch_size,
                              const common::ObIArray<ObExpr *> &udf_exprs, 
                              const common::ObIArray<ObExpr *> &input_exprs,
                              const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // init python udf cells
  for (int i = 0; i < udf_exprs.count() && OB_SUCC(ret); ++i) {
    ObExpr *udf_expr = udf_exprs.at(i);
    void *cell_ptr = static_cast<ObPythonUDFCell *>(alloc_.alloc(sizeof(ObPythonUDFCell)));
    ObPythonUDFCell *cell = new (cell_ptr) ObPythonUDFCell();
    if (cell == nullptr || OB_FAIL(cell->init(udf_expr, batch_size, capacity_, tenant_id)) ) {
      ret = OB_INIT_FAIL;
      LOG_WARN("Init Python UDF Cell failed.", K(ret));
    } else {
      cells_list_.add_last(cell);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(other_store_.init(input_exprs, batch_size, capacity_))) {
      ret = OB_INIT_FAIL;
      LOG_WARN("Init other input store failed.", K(ret));
    } else {
      stored_input_cnt_ = 0;
      stored_output_cnt_ = 0;
      output_idx_ = 0;
      batch_size_ = batch_size;
      tenant_id_ = tenant_id;
    }
  }
  return ret;
}

int ObPUStoreController::free()
{
  int ret = OB_SUCCESS;
  ObPythonUDFCell* header = cells_list_.get_header();
  for (ObPythonUDFCell* cell = cells_list_.get_first(); 
       cell != header && OB_SUCC(ret); 
       cell = cell->get_next()) {
    if (OB_FAIL(cell->free())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Free Python UDF Cell failed.", K(ret));
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(other_store_.free())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Free other input store failed.", K(ret));
    } else {
      // reset cnt & idx
      stored_input_cnt_ = 0;
      stored_output_cnt_ = 0;
      output_idx_ = 0;
    }
  }
  return ret;
}

int ObPUStoreController::store(ObEvalCtx &eval_ctx, ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  // do cells input store
  int64_t cnt = brs.size_ - brs.skip_->accumulate_bit_cnt(brs.size_);
  ObPythonUDFCell* header = cells_list_.get_header();
  for (ObPythonUDFCell* cell = cells_list_.get_first(); 
       cell != header && OB_SUCC(ret); 
       cell = cell->get_next()) {
    if (OB_FAIL(cell->do_store(eval_ctx, brs))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Do Python UDF Cell Store failed.", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // do other inputs store
    // 要判断skip进行数据重整，使用local allocator进行数据复制
    // load时要转为相应类型的vector
    // 下层传上来的数据不一定为uniform格式，需要save_vector()或cast_to_uniform()
    if (OB_FAIL(other_store_.save_vector(eval_ctx, brs))) {
    //if (OB_FAIL(other_store_.save_batch(eval_ctx, brs))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Save other input cols failed.", K(ret));
    } else {
      stored_input_cnt_ += cnt;
    }
  }
  return ret;
}

int ObPUStoreController::process()
{
  int ret = OB_SUCCESS;
  if (is_empty()) {
    /* do nothing */
  } else {
    //struct timeval t3, t4;
    //gettimeofday(&t3, NULL);
    
    ObPythonUDFCell* header = cells_list_.get_header();
    for (ObPythonUDFCell* cell = cells_list_.get_first(); 
        cell != header && OB_SUCC(ret); 
        cell = cell->get_next()) {
      if (cell->get_store_size() != stored_input_cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unsaved input rows.", K(ret));
      } else if (with_batch_control_ && OB_FAIL(cell->do_process())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Do Python UDF Cell process udf failed.", K(ret));
      } else if (!with_batch_control_ && OB_FAIL(cell->do_process_all())) {  // without predict size control
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Do Python UDF Cell process all udf failed.", K(ret));
      } else if (cell->get_result_size() != stored_input_cnt_){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unprocessed input rows.", K(ret));
      } else {
        cell->reset_input_store();
      }
    }
    if (OB_SUCC(ret)) {
      stored_output_cnt_ = stored_input_cnt_;
      stored_input_cnt_ = 0;
      output_idx_ = 0;
    }

    /*
    // pf time log
    gettimeofday(&t4, NULL);
    double timeuse2 = (t4.tv_sec - t3.tv_sec) * 1000000 + (double)(t4.tv_usec - t3.tv_usec); // usec
    double time_s2 = timeuse2 / 1000;
    std::fstream time_log2;
    time_log2.open("/home/test/experiments/oceanbase/revision/batch_control_overhead/process_time.log", std::ios::app);
    time_log2 << "process time: " << time_s2 << std::endl;
    time_log2.close(); */
  }
  return ret;
}

int ObPUStoreController::async_process(int id)
{
  int ret = OB_SUCCESS;
  if (is_empty()) {
    /* do nothing */
  } else {
    //struct timeval t3, t4;
    //gettimeofday(&t3, NULL);
    
    ObPythonUDFCell* header = cells_list_.get_header();
    for (ObPythonUDFCell* cell = cells_list_.get_first(); 
        cell != header && OB_SUCC(ret); 
        cell = cell->get_next()) {
      if (cell->get_store_size() != stored_input_cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unsaved input rows.", K(ret));
      } else if (OB_FAIL(cell->do_process_all())) {  // without predict size control
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Do Python UDF Cell process all udf failed.", K(ret));
      } else if (cell->get_result_size() != stored_input_cnt_){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unprocessed input rows.", K(ret));
      } else {
        cell->reset_input_store();
      }
    }
    if (OB_SUCC(ret)) {
      stored_output_cnt_ = stored_input_cnt_;
      stored_input_cnt_ = 0;
      output_idx_ = 0;
    }

    /*
    // pf time log
    gettimeofday(&t4, NULL);
    double timeuse2 = (t4.tv_sec - t3.tv_sec) * 1000000 + (double)(t4.tv_usec - t3.tv_usec); // usec
    double time_s2 = timeuse2 / 1000;
    std::fstream time_log2;
    time_log2.open("/home/test/experiments/oceanbase/revision/batch_control_overhead/process_time.log", std::ios::app);
    time_log2 << "process time: " << time_s2 << std::endl;
    time_log2.close(); */
  }
  if(ret == OB_SUCCESS) return id;
  return -1;
}

int ObPUStoreController::restore(ObEvalCtx &eval_ctx, ObBatchRows &brs, int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (stored_output_cnt_ <= 0 || !can_output()) { // Not enough output data
    /* do nothing */
  } else {
    // output_size > 0
    //int64_t output_size = batch_size_ < (stored_output_cnt_ - output_idx_) 
    //                     ? batch_size_ 
    //                     : (stored_output_cnt_ - output_idx_);
    int64_t output_size = std::min(max_row_cnt, stored_output_cnt_ - output_idx_); 
    ObPythonUDFCell* header = cells_list_.get_header();
    for (ObPythonUDFCell* cell = cells_list_.get_first(); 
        cell != header && OB_SUCC(ret); 
        cell = cell->get_next()) {
      if (OB_FAIL(cell->do_restore(eval_ctx, output_idx_, output_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Do Python UDF Cell restore failed.", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(other_store_.load_vector(eval_ctx, output_size))) {
      //if (OB_FAIL(other_store_.load_batch(eval_ctx, output_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Load other input cols failed.", K(ret));
      } else {
        output_idx_ += output_size;
        // reset batchrows
        brs.size_ = output_size;
        brs.reset_skip(output_size);
        brs.end_ = false;
      }
    }
    if (OB_SUCC(ret) && end_output()) {
      other_store_.reuse();
    }
  }
  return ret;
}

int ObPUStoreController::resize(int64_t size)
{
  int ret = OB_SUCCESS;
  ObPythonUDFCell* header = cells_list_.get_header();
  for (ObPythonUDFCell* cell = cells_list_.get_first(); 
      cell != header && OB_SUCC(ret); 
      cell = cell->get_next()) {
    if (OB_FAIL(cell->reset(size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Do Python UDF Cell resize failed.", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ret = other_store_.reset(size);
  }
  stored_input_cnt_ = 0;
  stored_output_cnt_ = 0;
  output_idx_ = 0;
  return ret;
}

} // end namespace sql
} // end namespace oceanbase