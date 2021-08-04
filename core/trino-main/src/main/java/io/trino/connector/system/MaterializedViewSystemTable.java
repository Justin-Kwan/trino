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
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.connector.system.jdbc.FilterUtil.tablePrefix;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.getMaterializedViews;
import static io.trino.metadata.MetadataListing.listCatalogs;
import static io.trino.metadata.MetadataListing.listSchemas;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static java.util.Objects.requireNonNull;

public class MaterializedViewSystemTable
        implements SystemTable
{
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final MaterializedViewRowAdapter materializedViewAdapter;

    @Inject
    public MaterializedViewSystemTable(
            Metadata metadata,
            AccessControl accessControl,
            MaterializedViewRowAdapter materializedViewAdapter)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.materializedViewAdapter = requireNonNull(materializedViewAdapter, "materializedViewAdapter is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return materializedViewAdapter.getTableLayout();
    }

    @Override
    public RecordCursor cursor(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession connectorSession,
            TupleDomain<Integer> constraint)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();

        List<MaterializedViewHandle> materializedViews = getAllMaterializedViews(session, constraint);
        List<Object[]> materializedViewRows = materializedViewAdapter.toTableRows(materializedViews);

        InMemoryRecordSet.Builder displayTable = InMemoryRecordSet.builder(getTableMetadata());
        materializedViewRows.forEach(s -> {
            System.out.println("rows : " + Arrays.toString(s));
            displayTable.addRow(s);
        });

        return displayTable.build().cursor();
    }

    private List<MaterializedViewHandle> getAllMaterializedViews(Session session, TupleDomain<Integer> constraint)
    {
        ImmutableList.Builder<MaterializedViewHandle> materializedViews = ImmutableList.builder();

        Optional<String> catalogNameFilter = tryGetSingleVarcharValue(constraint, 0);
        Optional<String> schemaNameFilter = tryGetSingleVarcharValue(constraint, 1);
        Optional<String> tableNameFilter = tryGetSingleVarcharValue(constraint, 2);

        Set<String> catalogNames = listCatalogs(session, metadata, accessControl, catalogNameFilter).keySet();

        for (String catalogName : catalogNames) {
            for (String schemaName : listSchemas(session, metadata, accessControl, catalogName, schemaNameFilter)) {
                QualifiedTablePrefix materializedViewPrefix = tablePrefix(catalogName, Optional.ofNullable(schemaName), tableNameFilter);
                materializedViews.addAll(getMaterializedViewsInSchema(session, materializedViewPrefix));
            }
        }

        return materializedViews.build();
    }

    private List<MaterializedViewHandle> getMaterializedViewsInSchema(Session session, QualifiedTablePrefix materializedViewPrefix)
    {
        ImmutableList.Builder<MaterializedViewHandle> materializedViewsInSchema = ImmutableList.builder();
        Map<SchemaTableName, ConnectorMaterializedViewDefinition> definitionsInSchema =
                getMaterializedViews(session, metadata, accessControl, materializedViewPrefix);

        for (Map.Entry<SchemaTableName, ConnectorMaterializedViewDefinition> definition : definitionsInSchema.entrySet()) {
            QualifiedObjectName name = new QualifiedObjectName(
                    materializedViewPrefix.getCatalogName(),
                    definition.getKey().getSchemaName(),
                    definition.getKey().getTableName());

            MaterializedViewFreshness freshness = metadata.getMaterializedViewFreshness(session, name);
            materializedViewsInSchema.add(new MaterializedViewHandle(name, definition.getValue(), freshness));
        }

        return materializedViewsInSchema.build();
    }
}
