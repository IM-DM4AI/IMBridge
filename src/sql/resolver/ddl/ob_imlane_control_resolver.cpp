/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_imlane_control_resolver.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
ObImlaneControlResolver::ObImlaneControlResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}
ObImlaneControlResolver::~ObImlaneControlResolver()
{
}
int ObImlaneControlResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  LOG_WARN("imlane : ObImlaneControlResolver::resolve");
  ObImlaneControlStmt *imlane_control_stmt = NULL;
  if (OB_ISNULL(session_info_)
      || OB_ISNULL(schema_checker_)
      || OB_ISNULL(allocator_)
      || (T_IMLANE_CONTROL != parse_tree.type_)
    ) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  }
  if (OB_SUCC(ret)) {
    // dll udf
    if (OB_ISNULL(imlane_control_stmt = create_stmt<ObImlaneControlStmt>())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(ERROR, "imlane control stmt failed");
    }
  }
  if (OB_SUCC(ret)) {
    const uint64_t tenant_id = session_info_->get_effective_tenant_id();
    obrpc::ObImlaneControlArg &arg = imlane_control_stmt->get_imlane_control_arg();
    arg.tenant_id_=tenant_id;
    if(parse_tree.num_child_==0){
      arg.is_launch_=false;
    }
    else{
      arg.is_launch_=true;

      // 构造 std::string
      std::string num_str(parse_tree.children_[1]->str_value_, parse_tree.children_[1]->str_len_);
      // 转换为 double
      double arg2 = std::stod(num_str);

      arg.launch_arg_1_=parse_tree.children_[0]->value_;
      arg.launch_arg_2_=arg2;
    }
  }
  return ret;
}
}
}