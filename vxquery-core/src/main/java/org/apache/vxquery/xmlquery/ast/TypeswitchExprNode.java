/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.vxquery.xmlquery.ast;

import java.util.List;

import org.apache.vxquery.util.SourceLocation;

public class TypeswitchExprNode extends ASTNode {
    private ASTNode switchExpr;
    private List<CaseClauseNode> caseClauses;
    private QNameNode defaultVar;
    private ASTNode defaultClause;

    public TypeswitchExprNode(SourceLocation loc) {
        super(loc);
    }

    @Override
    public ASTTag getTag() {
        return ASTTag.TYPESWITCH_EXPRESSION;
    }

    public ASTNode getSwitchExpr() {
        return switchExpr;
    }

    public void setSwitchExpr(ASTNode switchExpr) {
        this.switchExpr = switchExpr;
    }

    public List<CaseClauseNode> getCaseClauses() {
        return caseClauses;
    }

    public void setCaseClauses(List<CaseClauseNode> caseClauses) {
        this.caseClauses = caseClauses;
    }

    public QNameNode getDefaultVar() {
        return defaultVar;
    }

    public void setDefaultVar(QNameNode defaultVar) {
        this.defaultVar = defaultVar;
    }

    public ASTNode getDefaultClause() {
        return defaultClause;
    }

    public void setDefaultClause(ASTNode defaultClause) {
        this.defaultClause = defaultClause;
    }
}
