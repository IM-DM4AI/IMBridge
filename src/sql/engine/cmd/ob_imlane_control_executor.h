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
#ifndef OB_IMLANE_CONTROL_EXECUTOR_H_
#define OB_IMLANE_CONTROL_EXECUTOR_H_
#include "common/object/ob_object.h"
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObExprCtx;
// namespace sqlclient
// {
class ObMySQLProxy;
// }
}
namespace sql
{
class ObExecContext;
class ObRawExpr;
class ObImlaneControlStmt;
class ObIMLaneControlExecutor
{
public:
  ObIMLaneControlExecutor(){}
  virtual ~ObIMLaneControlExecutor(){}
  int execute(ObExecContext &ctx, ObImlaneControlStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObIMLaneControlExecutor);
};
}
}
#endif //OB_IMLANE_CONTROL_EXECUTOR_H_