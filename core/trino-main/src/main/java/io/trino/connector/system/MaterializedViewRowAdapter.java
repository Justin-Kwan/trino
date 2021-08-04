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

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

/**
 * Adapter to convert a materialized view representation into a single
 * row to be displayed on the system metadata table.
 */
public interface MaterializedViewRowAdapter
{
    ConnectorTableMetadata getTableLayout();

    SchemaTableName getTableName();

    List<ColumnMetadata> getTableColumns();

    List<Object[]> toTableRows(List<MaterializedViewDto> materializedViewRowDto);
}
