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

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.MetadataListing.getMaterializedViews;
import static io.trino.metadata.MetadataListing.listCatalogs;
import static io.trino.metadata.MetadataListing.listMaterializedViews;
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

        List<QualifiedObjectName> materializedViewNames = getAllMaterializedViewNames(session);
        List<MaterializedViewDto> materializedViews = getMaterializedViewsByName(session, materializedViewNames);
        List<Object[]> materializedViewRows = materializedViewAdapter.toTableRows(materializedViews);

        InMemoryRecordSet.Builder displayTable = InMemoryRecordSet.builder(getTableMetadata());
        materializedViewRows.forEach(displayTable::addRow);

        return displayTable.build().cursor();
    }

    private List<QualifiedObjectName> getAllMaterializedViewNames(Session session)
    {
        ImmutableList.Builder<QualifiedObjectName> materializedViewNames = ImmutableList.builder();
        Set<String> catalogNames = listCatalogs(session, metadata, accessControl).keySet();

        for (String catalogName : catalogNames) {
            Set<String> schemaNamesInCatalog = listSchemas(session, metadata, accessControl, catalogName);

            for (String schemaName : schemaNamesInCatalog) {
                QualifiedTablePrefix materializedViewPrefix = new QualifiedTablePrefix(catalogName, schemaName);
                materializedViewNames.addAll(getMaterializedViewNamesInSchema(session, materializedViewPrefix));
            }
        }

        return materializedViewNames.build();
    }

    private List<QualifiedObjectName> getMaterializedViewNamesInSchema(Session session, QualifiedTablePrefix prefix)
    {
        Set<SchemaTableName> materializedViewNames = listMaterializedViews(session, metadata, accessControl, prefix);

        return materializedViewNames.stream()
                .map(materializedView -> new QualifiedObjectName(
                        prefix.getCatalogName(),
                        materializedView.getSchemaName(),
                        materializedView.getTableName()))
                .collect(toImmutableList());
    }

    private List<MaterializedViewDto> getMaterializedViewsByName(Session session, List<QualifiedObjectName> materializedViewNames)
    {
        ImmutableList.Builder<MaterializedViewDto> materializedViews = ImmutableList.builder();

        for (QualifiedObjectName name : materializedViewNames) {
            ConnectorMaterializedViewDefinition definition = getMaterializedViews(session, metadata, accessControl, name
                    .asQualifiedTablePrefix())
                    .values().iterator().next();

            MaterializedViewFreshness freshness = metadata.getMaterializedViewFreshness(session, name);
            materializedViews.add(new MaterializedViewDto(name, definition, freshness));
        }

        return materializedViews.build();
    }
}
