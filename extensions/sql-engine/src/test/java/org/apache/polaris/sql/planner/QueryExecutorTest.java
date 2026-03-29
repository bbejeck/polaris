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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class QueryExecutorTest {

    @Mock Catalog catalog;
    @Mock Table table;
    @Mock Snapshot snapshot;
    @Mock TableScan tableScan;

    QueryExecutor executor;

    static final Namespace NS = Namespace.of("prod");
    static final TableIdentifier TABLE_ID = TableIdentifier.of(NS, "events");
    static final String NAMESPACED_TABLE = "prod.events";

    @BeforeEach
    void setUp() {
        executor = new QueryExecutor(catalog);
        lenient().when(catalog.loadTable(TABLE_ID)).thenReturn(table);
    }

    @Test
    void showTablesReturnsNamespaceAndCount() {
        List<TableIdentifier> tables = List.of(TABLE_ID, TableIdentifier.of(NS, "logs"));
        when(catalog.listTables(NS)).thenReturn(tables);

        Object result = executor.execute(new QueryPlan.ShowTables("prod"));

        assertThat(result).isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) result;
        assertThat(map).containsEntry("namespace", "prod");
        assertThat(map).containsEntry("tableCount", 2);
        assertThat(map.get("tables")).isEqualTo(tables);
    }

    @Test
    void describeStatsReturnsSnapshotInfo() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()));
        when(table.currentSnapshot()).thenReturn(snapshot);
        when(table.snapshots()).thenReturn(List.of(snapshot, snapshot));
        when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
        when(table.schema()).thenReturn(schema);
        when(snapshot.snapshotId()).thenReturn(42L);

        Object result = executor.execute(new QueryPlan.DescribeStats(NAMESPACED_TABLE));

        assertThat(result).isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) result;
        assertThat(map).containsEntry("snapshotCount", 2L);
        assertThat(map).containsEntry("currentSnapshotId", 42L);
    }

    @Test
    void describeStatsHandlesNoSnapshot() {
        when(table.currentSnapshot()).thenReturn(null);
        when(table.snapshots()).thenReturn(Collections.emptyList());
        when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
        when(table.schema()).thenReturn(new Schema());

        Object result = executor.execute(new QueryPlan.DescribeStats(NAMESPACED_TABLE));

        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) result;
        assertThat(map).containsEntry("snapshotCount", 0L);
        assertThat(map).containsEntry("currentSnapshotId", -1L);
    }

    @Test
    void showLocationReturnsTableLocation() {
        when(table.location()).thenReturn("s3://my-bucket/warehouse/prod/events");

        Object result = executor.execute(new QueryPlan.ShowLocation(NAMESPACED_TABLE));

        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) result;
        assertThat(map).containsEntry("location", "s3://my-bucket/warehouse/prod/events");
    }

    @Test
    void showPoliciesReturnsTableProperties() {
        Map<String, String> props = Map.of(
                "write.format.default", "parquet",
                "polaris.policy.retention", "30d");
        when(table.properties()).thenReturn(props);

        Object result = executor.execute(new QueryPlan.ShowPolicies(NAMESPACED_TABLE));

        assertThat(result).isEqualTo(props);
    }

    @Test
    void diagnoseReturnsSmallFileCount() {
        DataFile smallFile = mock(DataFile.class);
        DataFile largeFile = mock(DataFile.class);
        FileScanTask smallTask = mock(FileScanTask.class);
        FileScanTask largeTask = mock(FileScanTask.class);

        long threshold = 128 * 1024 * 1024L;
        when(table.currentSnapshot()).thenReturn(snapshot);
        when(table.newScan()).thenReturn(tableScan);
        when(tableScan.planFiles()).thenReturn(CloseableIterable.withNoopClose(
                List.of(smallTask, largeTask)));
        when(smallTask.file()).thenReturn(smallFile);
        when(largeTask.file()).thenReturn(largeFile);
        when(smallFile.fileSizeInBytes()).thenReturn(1024L);
        when(largeFile.fileSizeInBytes()).thenReturn(threshold + 1);

        Object result = executor.execute(new QueryPlan.Diagnose(NAMESPACED_TABLE));

        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) result;
        assertThat(map).containsEntry("smallFileThresholdBytes", threshold);
        assertThat(map).containsEntry("smallFileCount", 1L);
    }

    @Test
    void diagnoseWithNoSnapshotReturnsZeroCount() {
        when(table.currentSnapshot()).thenReturn(null);

        Object result = executor.execute(new QueryPlan.Diagnose(NAMESPACED_TABLE));

        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) result;
        assertThat(map).containsEntry("smallFileCount", 0L);
    }
}
