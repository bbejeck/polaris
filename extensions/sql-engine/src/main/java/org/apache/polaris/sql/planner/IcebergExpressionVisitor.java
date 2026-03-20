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

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.polaris.sql.grammar.IcebergSQLBaseVisitor;
import org.apache.polaris.sql.grammar.IcebergSQLParser;

import java.util.List;

/**
 * User: Bill Bejeck
 * Date: 2/21/26
 * Time: 9:41 AM
 */
public class IcebergExpressionVisitor extends IcebergSQLBaseVisitor<Expression> {
    @Override
    public Expression visitAndPred(IcebergSQLParser.AndPredContext ctx) {
        return Expressions.and(visit(ctx.predicate(0)), visit(ctx.predicate(1)));
    }

    @Override
    public Expression visitOrPred(IcebergSQLParser.OrPredContext ctx) {
        return Expressions.or(visit(ctx.predicate(0)), visit(ctx.predicate(1)));
    }
    @Override
    public Expression visitNotPred(IcebergSQLParser.NotPredContext ctx) {
        return Expressions.not(visit(ctx.predicate()));
    }

    @Override
    public Expression visitInPred(IcebergSQLParser.InPredContext ctx) {
        String col = ctx.expression().getText();
        List<Object> values = ctx.literal().stream().map(this::parseLiteral).toList();
        return Expressions.in(col, values);
    }

    @Override
    public Expression visitNotInPred(IcebergSQLParser.NotInPredContext ctx) {
        String col = ctx.expression().getText();
        List<Object> values = ctx.literal().stream().map(this::parseLiteral).toList();
        return Expressions.notIn(col, values);
    }

    @Override
    public Expression visitIsNullPred(IcebergSQLParser.IsNullPredContext ctx) {
        String col = ctx.expression().getText();
        boolean notNull = ctx.NOT() != null;
        return notNull ? Expressions.notNull(col) : Expressions.isNull(col);
    }

    @Override
    public Expression visitParenPred(IcebergSQLParser.ParenPredContext ctx) {
        return visit(ctx.predicate());
    }

    @Override
    public Expression visitComparisonPred(IcebergSQLParser.ComparisonPredContext ctx) {
        String col = ctx.expression(0).getText();
        Object value = parseLiteralExp(ctx.expression(1));
        return switch(ctx.op().getText()) {
            case "<" -> Expressions.lessThan(col, value);
            case "<=" -> Expressions.lessThanOrEqual(col, value);
            case ">" -> Expressions.greaterThan(col, value);
            case ">=" -> Expressions.greaterThanOrEqual(col, value);
            case "=" -> Expressions.equal(col, value);
            case "!=" -> Expressions.notEqual(col, value);
            default -> throw new IllegalArgumentException("Unknown comparison operator: " + ctx.op().getText());
        };
    }

    private Object parseLiteralExp(IcebergSQLParser.ExpressionContext ctx) {
        if (ctx instanceof IcebergSQLParser.LiteralExprContext literalCtx) {
            return parseLiteral(literalCtx.literal());
        }
        throw new IllegalArgumentException("Expected literal, got: " + ctx.getText());
    }

    private Object parseLiteral(IcebergSQLParser.LiteralContext ctx) {
        switch (ctx) {
            case IcebergSQLParser.IntLiteralContext intCtx -> {
                return Long.parseLong(intCtx.getText());
            }
            case IcebergSQLParser.FloatLiteralContext floatCtx -> {
                return Double.parseDouble(floatCtx.getText());
            }
            case IcebergSQLParser.StringLiteralContext stringCtx -> {
                String raw = stringCtx.getText();
                return raw.substring(1, raw.length() - 1).replace("''", "'");
            }
            case IcebergSQLParser.TrueLiteralContext trueLiteralContext -> {
                return true;
            }
            case IcebergSQLParser.FalseLiteralContext falseLiteralContext -> {
                return false;
            }
            default -> throw new IllegalArgumentException("Unknown literal type: " + ctx.getText());
        }
    }
}
