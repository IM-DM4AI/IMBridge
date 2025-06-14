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
#ifndef _OB_IMLANE_CONTROL_RESOLVER_H
#define _OB_IMLANE_CONTROL_RESOLVER_H 1
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_imlane_control_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObImlaneControlResolver : public ObDDLResolver
{
public:
  explicit ObImlaneControlResolver(ObResolverParams &params);
  virtual ~ObImlaneControlResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObImlaneControlResolver);
};
}
}
#endif /* _OB_IMLANE_CONTROL_RESOLVER_H */