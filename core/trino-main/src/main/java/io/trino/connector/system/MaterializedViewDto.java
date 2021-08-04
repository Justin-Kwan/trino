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

import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.MaterializedViewFreshness;

public class MaterializedViewDto
{
    // required because optional catalog and schema names
    // are flaky in ConnectorMaterializedViewDefinition.
    private final QualifiedObjectName fullyQualifiedName;
    private final ConnectorMaterializedViewDefinition definition;
    private final MaterializedViewFreshness freshness;

    public MaterializedViewDto(
            QualifiedObjectName fullyQualifiedName,
            ConnectorMaterializedViewDefinition definition,
            MaterializedViewFreshness freshness)
    {
        this.fullyQualifiedName = fullyQualifiedName;
        this.definition = definition;
        this.freshness = freshness;
    }

    public boolean isFresh()
    {
        return freshness.isMaterializedViewFresh();
    }

    public String getName()
    {
        return fullyQualifiedName.getObjectName();
    }

    public String getCatalogName()
    {
        return fullyQualifiedName.getCatalogName();
    }

    public String getSchemaName()
    {
        return fullyQualifiedName.getSchemaName();
    }

    public String getTableType()
    {
        return "MATERIALIZED VIEW";
    }

    public String getOwner()
    {
        return definition.getOwner();
    }

    public String getComment()
    {
        return definition.getComment().orElse("");
    }

    public ConnectorMaterializedViewDefinition getDefinition()
    {
        return definition;
    }

    public String getSqlDefinition()
    {
        StringBuilder sqlDefinition = new StringBuilder()
                .append("CREATE OR REPLACE MATERIALIZED VIEW")
                .append("\n ")
                .append(fullyQualifiedName.toString())
                .append("\n");

        definition.getComment().ifPresent(commentText -> sqlDefinition
                .append("COMMENT")
                .append("\n '")
                .append(commentText)
                .append("'\n"));

        sqlDefinition
                .append("AS ")
                .append(definition.getOriginalSql())
                .append(";");

        return sqlDefinition.toString();
    }
}
