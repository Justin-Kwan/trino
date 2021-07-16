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
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.List;
import java.util.Set;

import static io.trino.metadata.MetadataListing.getMaterializedViews;
import static io.trino.metadata.MetadataListing.listCatalogs;
import static io.trino.metadata.MetadataListing.listSchemas;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class MaterializedViewSystemTable
        implements SystemTable
{
    public static final SchemaTableName QUALIFIED_NAME = new SchemaTableName("metadata", "materialized_views");
    public static final ConnectorTableMetadata TABLE = tableMetadataBuilder(QUALIFIED_NAME)
            .column("table_catalog", createUnboundedVarcharType())
            .column("table_schema", createUnboundedVarcharType())
            .column("materialized_view_name", createUnboundedVarcharType())
            .column("table_type", createUnboundedVarcharType())
            .column("is_fresh", BOOLEAN)
            .column("refreshed_on", createUnboundedVarcharType())
            .column("owner", createUnboundedVarcharType())
            .column("text", createUnboundedVarcharType())
            .column("materialized_view_comment", createUnboundedVarcharType())
            .build();

    @Inject
    public MaterializedViewSystemTable(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    private final Metadata metadata;
    private final AccessControl accessControl;

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        InMemoryRecordSet.Builder outputTable = InMemoryRecordSet.builder(TABLE);
        Set<String> catalogNames = listCatalogs(session, metadata, accessControl).keySet();

        for (String catalogName : catalogNames) {
            displayMaterializedViewsInCatalog(session, catalogName, outputTable);
        }

        return outputTable.build().cursor();
    }

    private void displayMaterializedViewsInCatalog(Session session, String catalogName, InMemoryRecordSet.Builder displayTable)
    {
        Set<String> schemasInCatalog = listSchemas(session, metadata, accessControl, catalogName);

        for (String schema : schemasInCatalog) {
            QualifiedTablePrefix materializedViewPrefix = new QualifiedTablePrefix(catalogName, schema);
            displayMaterializedViewsInSchema(session, materializedViewPrefix, displayTable);
        }
    }

    private void displayMaterializedViewsInSchema(
            Session session,
            QualifiedTablePrefix materializedViewPrefix,
            InMemoryRecordSet.Builder displayTable)
    {
        List<ConnectorMaterializedViewDefinition> materializedViewsInSchema =
                getMaterializedViewDefinitions(session, materializedViewPrefix);

        for (ConnectorMaterializedViewDefinition materializedView : materializedViewsInSchema) {
            displayTable.addRow(
                    materializedViewPrefix.getCatalogName(),
                    materializedViewPrefix.getSchemaName(),
                    materializedViewPrefix.getTableName(),
                    "MATERIALIZED VIEW",
                    isMaterializedViewFresh(session, materializedViewPrefix),
                    "...",
                    materializedView.getOwner(),
                    materializedView.getOriginalSql(),
                    materializedView.getComment());
        }
    }

    private boolean isMaterializedViewFresh(Session session, QualifiedTablePrefix tablePrefix)
    {
        return metadata.getMaterializedViewFreshness(
                session,
                new QualifiedObjectName(
                        tablePrefix.getCatalogName(),
                        tablePrefix.getSchemaName().get(),
                        tablePrefix.getTableName().get()))
                .isMaterializedViewFresh();
    }

    private List<ConnectorMaterializedViewDefinition> getMaterializedViewDefinitions(
            Session session,
            QualifiedTablePrefix tablePrefix)
    {
        return ImmutableList.copyOf(
                getMaterializedViews(session, metadata, accessControl, tablePrefix).values());
    }
}
