/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.connector.system;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public class TrinoMaterializedViewAdapter
        implements MaterializedViewRowAdapter
{
    public ConnectorTableMetadata getTableLayout()
    {
        return new ConnectorTableMetadata(getTableName(), getTableColumns());
    }

    public SchemaTableName getTableName()
    {
        return new SchemaTableName("metadata", "materialized_views");
    }

    public List<ColumnMetadata> getTableColumns()
    {
        return ImmutableList.of(
                new ColumnMetadata("catalog_name", createUnboundedVarcharType()),
                new ColumnMetadata("schema_name", createUnboundedVarcharType()),
                new ColumnMetadata("name", createUnboundedVarcharType()),
                new ColumnMetadata("storage_catalog", createUnboundedVarcharType()),
                new ColumnMetadata("storage_schema", createUnboundedVarcharType()),
                new ColumnMetadata("storage_table", createUnboundedVarcharType()),
                new ColumnMetadata("is_fresh", BOOLEAN),
                new ColumnMetadata("owner", createUnboundedVarcharType()),
                new ColumnMetadata("comment", createUnboundedVarcharType()),
                new ColumnMetadata("text", createUnboundedVarcharType()));
    }

    @Override
    public List<Object[]> toTableRows(List<MaterializedViewHandle> materializedViewHandles)
    {
        return materializedViewHandles.stream()
                .map(materializedViewHandle -> new Object[] {
                        materializedViewHandle.getCatalogName(),
                        materializedViewHandle.getSchemaName(),
                        materializedViewHandle.getName(),
                        materializedViewHandle.getStorageCatalog(),
                        materializedViewHandle.getStorageSchema(),
                        materializedViewHandle.getStorageTable(),
                        materializedViewHandle.isFresh(),
                        materializedViewHandle.getOwner(),
                        materializedViewHandle.getComment(),
                        materializedViewHandle.getOriginalSql()
                })
                .collect(toImmutableList());
    }
}
