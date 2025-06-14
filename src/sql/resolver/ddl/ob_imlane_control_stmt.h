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
#ifndef _IMLANE_CONTROL_STMT_H
#define _IMLANE_CONTROL_STMT_H 1
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObImlaneControlStmt : public ObDDLStmt
{
public:
  ObImlaneControlStmt() :
      ObDDLStmt(stmt::T_IMLANE_CONTROL),
      imlane_control_arg_()
  {}
  ~ObImlaneControlStmt() {}
  void set_tenant_id(uint64_t tenant_id) { imlane_control_arg_.tenant_id_ = tenant_id; }

  obrpc::ObImlaneControlArg &get_imlane_control_arg() { return imlane_control_arg_; }
  obrpc::ObDDLArg &get_ddl_arg() { return imlane_control_arg_; }
  TO_STRING_KV(K_(imlane_control_arg));
private:
  obrpc::ObImlaneControlArg imlane_control_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObImlaneControlStmt);
};
}
}
#endif /* _IMLANE_CONTROL_STMT_H */