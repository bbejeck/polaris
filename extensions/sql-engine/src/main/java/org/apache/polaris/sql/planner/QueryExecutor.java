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

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * Executes a {@link QueryPlan} against an Iceberg {@link Catalog}.
 * Dispatches each plan type to the appropriate Iceberg API calls and returns
 * the result as a plain Java object (typically a {@code Map}).
 */
public class QueryExecutor {

    private final Catalog catalog;

    public QueryExecutor(Catalog catalog) {
        this.catalog = catalog;
    }

    public Object execute(QueryPlan plan) {
        return switch (plan) {
            case QueryPlan.Select s         -> executeSelect(s);
            case QueryPlan.ShowTables st    -> showTables(st);
            case QueryPlan.DescribeStats d  -> describeStats(d);
            case QueryPlan.ShowLocation sl  -> showLocation(sl);
            case QueryPlan.ShowPolicies sp  -> showPolicies(sp);
            case QueryPlan.Diagnose diag    -> diagnose(diag);
        };
    }

    // Use-case 1: count + list tables under a namespace
    private Object showTables(QueryPlan.ShowTables plan) {
        Namespace ns = Namespace.of(plan.namespace().split("\\."));
        List<TableIdentifier> tables = catalog.listTables(ns);
        return Map.of(
                "namespace", plan.namespace(),
                "tableCount", tables.size(),
                "tables", tables
        );
    }

    // Use-case 2: snapshot count, current snapshot id, partition spec, schema
    private Object describeStats(QueryPlan.DescribeStats plan) {
        Table table = loadTable(plan.namespacedTable());
        var currentSnapshot = table.currentSnapshot();
        long snapshotCount = StreamSupport.stream(table.snapshots().spliterator(), false).count();
        return Map.of(
                "snapshotCount", snapshotCount,
                "currentSnapshotId", currentSnapshot != null ? currentSnapshot.snapshotId() : -1L,
                "partitionSpec", table.spec().toString(),
                "schema", table.schema().toString()
        );
    }

    // Use-case 3: storage location
    private Object showLocation(QueryPlan.ShowLocation plan) {
        Table table = loadTable(plan.namespacedTable());
        return Map.of("location", table.location());
    }

    // Use-case 4: effective policies via table properties (polaris.policy.* prefix only)
    private static final String POLICY_PREFIX = "polaris.policy.";

    private Object showPolicies(QueryPlan.ShowPolicies plan) {
        Table table = loadTable(plan.namespacedTable());
        Map<String, String> policies = new HashMap<>();
        table.properties().forEach((k, v) -> {
            if (k.startsWith(POLICY_PREFIX)) {
                policies.put(k, v);
            }
        });
        return policies;
    }

    /**
     * Files smaller than this threshold (128 MiB) are considered "small" by the diagnostics scan.
     * This matches the default Iceberg target file size.
     */
    private static final long SMALL_FILE_THRESHOLD_BYTES = 128 * 1024 * 1024L;

    // Use-case 5: small-file diagnostics via manifest scanning
    private Object diagnose(QueryPlan.Diagnose plan) {
        Table table = loadTable(plan.namespacedTable());
        long smallFileCount = 0;
        if (table.currentSnapshot() != null) {
            try (var tasks = table.newScan().planFiles()) {
                for (var fileScanTask : tasks) {
                    if (fileScanTask.file().fileSizeInBytes() < SMALL_FILE_THRESHOLD_BYTES) {
                        smallFileCount++;
                    }
                }
            } catch (java.io.IOException e) {
                throw new RuntimeException("Failed to close file scan tasks during diagnose", e);
            }
        }
        return Map.of(
                "smallFileThresholdBytes", SMALL_FILE_THRESHOLD_BYTES,
                "smallFileCount", smallFileCount
        );
    }

    /**
     * Not supported: {@link QueryExecutor} handles metadata operations only.
     * Use {@link IcebergRestQueryExecutor} to execute SELECT plans.
     *
     * @throws IllegalArgumentException always
     */
    private Object executeSelect(QueryPlan.Select plan) {
        throw new IllegalArgumentException(
                "QueryExecutor does not support SELECT plans; use IcebergRestQueryExecutor");
    }

    private Table loadTable(String namespacedTable) {
        String[] parts = namespacedTable.split("\\.");
        if (parts.length < 2) {
            throw new IllegalArgumentException(
                    "Table name must be namespace-qualified (e.g. 'ns.table'), got: " + namespacedTable);
        }
        Namespace ns = Namespace.of(Arrays.copyOf(parts, parts.length - 1));
        return catalog.loadTable(TableIdentifier.of(ns, parts[parts.length - 1]));
    }
}
