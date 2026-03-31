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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.expressions.Expression;
import org.junit.jupiter.api.Test;

class SqlToQueryPlanTest {

    private final SqlToQueryPlan translator = new SqlToQueryPlan();

    @Test
    void selectStarProducesSelectPlan() {
        QueryPlan plan = translator.translate("SELECT * FROM prod.events");

        assertThat(plan).isInstanceOf(QueryPlan.Select.class);
        QueryPlan.Select select = (QueryPlan.Select) plan;
        assertThat(select.namespacedTable()).isEqualTo("prod.events");
        assertThat(select.projectedColumns()).isEmpty();
        assertThat(select.filter()).isNull();
        assertThat(select.limit()).isEmpty();
    }

    @Test
    void selectWithColumnsFilterAndLimit() {
        QueryPlan plan = translator.translate(
                "SELECT region, sales FROM prod.events WHERE sales > 1000 LIMIT 50");

        assertThat(plan).isInstanceOf(QueryPlan.Select.class);
        QueryPlan.Select select = (QueryPlan.Select) plan;
        assertThat(select.namespacedTable()).isEqualTo("prod.events");
        assertThat(select.projectedColumns()).containsExactly("region", "sales");
        assertThat(select.filter()).isNotNull();
        assertThat(select.filter().op()).isEqualTo(Expression.Operation.GT);
        assertThat(select.limit()).hasValue(50L);
    }

    @Test
    void showTablesProducesShowTablesPlan() {
        QueryPlan plan = translator.translate("SHOW TABLES IN prod.logs");

        assertThat(plan).isInstanceOf(QueryPlan.ShowTables.class);
        assertThat(((QueryPlan.ShowTables) plan).namespace()).isEqualTo("prod.logs");
    }

    @Test
    void describeStatsProducesDescribeStatsPlan() {
        QueryPlan plan = translator.translate("DESCRIBE STATS prod.events");

        assertThat(plan).isInstanceOf(QueryPlan.DescribeStats.class);
        assertThat(((QueryPlan.DescribeStats) plan).namespacedTable()).isEqualTo("prod.events");
    }

    @Test
    void showLocationProducesShowLocationPlan() {
        QueryPlan plan = translator.translate("SHOW TABLE LOCATION prod.events");

        assertThat(plan).isInstanceOf(QueryPlan.ShowLocation.class);
        assertThat(((QueryPlan.ShowLocation) plan).namespacedTable()).isEqualTo("prod.events");
    }

    @Test
    void showPoliciesProducesShowPoliciesPlan() {
        QueryPlan plan = translator.translate("SHOW TABLE POLICIES prod.events");

        assertThat(plan).isInstanceOf(QueryPlan.ShowPolicies.class);
        assertThat(((QueryPlan.ShowPolicies) plan).namespacedTable()).isEqualTo("prod.events");
    }

    @Test
    void diagnoseTableProducesDiagnosePlan() {
        QueryPlan plan = translator.translate("DIAGNOSE TABLE prod.events");

        assertThat(plan).isInstanceOf(QueryPlan.Diagnose.class);
        assertThat(((QueryPlan.Diagnose) plan).namespacedTable()).isEqualTo("prod.events");
    }

    @Test
    void deeplyNestedNamespace() {
        QueryPlan plan = translator.translate("SHOW TABLES IN catalog.schema.db");

        assertThat(plan).isInstanceOf(QueryPlan.ShowTables.class);
        assertThat(((QueryPlan.ShowTables) plan).namespace()).isEqualTo("catalog.schema.db");
    }

    @Test
    void invalidSqlThrowsIllegalArgumentException() {
        assertThatThrownBy(() -> translator.translate("NOT VALID SQL !!!"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void syntaxErrorThrowsIllegalArgumentException() {
        assertThatThrownBy(() -> translator.translate("SELECT FROM"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void selectWithAndPredicate() {
        QueryPlan plan = translator.translate(
                "SELECT id FROM ns.tbl WHERE id > 0 AND id < 100");

        assertThat(plan).isInstanceOf(QueryPlan.Select.class);
        QueryPlan.Select select = (QueryPlan.Select) plan;
        assertThat(select.filter()).isNotNull();
        assertThat(select.filter().op()).isEqualTo(Expression.Operation.AND);
    }
}
