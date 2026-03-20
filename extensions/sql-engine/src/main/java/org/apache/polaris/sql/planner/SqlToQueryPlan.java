/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.sql.planner;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.iceberg.expressions.Expression;
import org.apache.polaris.sql.grammar.IcebergSQLLexer;
import org.apache.polaris.sql.grammar.IcebergSQLParser;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * User: Bill Bejeck
 * Date: 2/21/26
 * Time: 1:05 PM
 */
public class SqlToQueryPlan {

    public QueryPlan translate(String sql) {
        var lexer = new IcebergSQLLexer(CharStreams.fromString(sql));
        var tokens = new CommonTokenStream(lexer);
        var parser = new IcebergSQLParser(tokens);

        IcebergSQLParser.QueryContext queryCtx = parser.query();

        String table = queryCtx.tableRef().getText();
        
        List<String> columns = new ArrayList<>();
        if (queryCtx.columnList() instanceof IcebergSQLParser.NamedColumnsContext columnListCtx) {
            columns = new ArrayList<>(columnListCtx.column().stream().map(IcebergSQLParser.ColumnContext::getText).toList());
        }
        Expression filter = null;
        if (queryCtx.predicate() != null) {
            filter = new IcebergExpressionVisitor().visit(queryCtx.predicate());
        }

        OptionalLong limit = OptionalLong.empty();
        if (queryCtx.INTEGER_LITERAL() != null) {
            limit = OptionalLong.of(Long.parseLong(queryCtx.INTEGER_LITERAL().getText()));
        }
        
        return new QueryPlan(table, columns, filter, limit);
    }
}
