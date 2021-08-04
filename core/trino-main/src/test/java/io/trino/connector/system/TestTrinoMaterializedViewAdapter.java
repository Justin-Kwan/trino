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
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.type.TypeId;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestTrinoMaterializedViewAdapter
{
    private static final String MATERIALIZED_VIEW_COMMENT_1 = "test comment 1";
    private static final String MATERIALIZED_VIEW_COMMENT_2 = "test comment 2";
    private static final String EMPTY_COMMENT = "";
    private static final String MATERIALIZED_VIEW_OWNER = "test_owner";
    private static final String SELECT_SOURCE_TABLE_SQL = "SELECT col1, col2, col3 FROM test_table";

    private static final QualifiedObjectName FULLY_QUALIFIED_NAME_1 = new QualifiedObjectName(
            "test_catalog_name_1",
            "test_schema_name_1",
            "test_schema_name_1");
    private static final QualifiedObjectName FULLY_QUALIFIED_NAME_2 = new QualifiedObjectName(
            "test_catalog_name_2",
            "test_schema_name_2",
            "test_schema_name_2");

    private static final MaterializedViewRowAdapter MATERIALIZED_VIEW_ROW_ADAPTER =
            new TrinoMaterializedViewAdapter();

    private static MaterializedViewDto createMaterializedViewDto(
            QualifiedObjectName fullyQualifiedName,
            Optional<String> comment,
            boolean isFresh)
    {
        return new MaterializedViewDto(
                fullyQualifiedName,
                new ConnectorMaterializedViewDefinition(
                        SELECT_SOURCE_TABLE_SQL,
                        Optional.of(fullyQualifiedName.asCatalogSchemaTableName()),
                        Optional.of(fullyQualifiedName.getCatalogName()),
                        Optional.of(fullyQualifiedName.getSchemaName()),
                        ImmutableList.of(new ConnectorMaterializedViewDefinition.Column("test_column", TypeId.of("test_type"))),
                        comment,
                        MATERIALIZED_VIEW_OWNER,
                        ImmutableMap.of()),
                new MaterializedViewFreshness(isFresh));
    }

    private static Object createMaterializedViewRow(
            QualifiedObjectName fullyQualifiedName,
            Optional<String> comment,
            boolean isFresh)
    {
        return new Object[] {
                fullyQualifiedName.getCatalogName(),
                fullyQualifiedName.getSchemaName(),
                fullyQualifiedName.getObjectName(),
                "MATERIALIZED VIEW",
                isFresh,
                MATERIALIZED_VIEW_OWNER,
                comment.orElse(""),
                getMaterializedViewSqlDefinition(fullyQualifiedName, comment)
        };
    }

    public static String getMaterializedViewSqlDefinition(QualifiedObjectName fullyQualifiedName, Optional<String> comment)
    {
        StringBuilder sqlDefinition = new StringBuilder()
                .append("CREATE OR REPLACE MATERIALIZED VIEW")
                .append("\n ")
                .append(fullyQualifiedName.toString())
                .append("\n");

        comment.ifPresent(commentText -> sqlDefinition
                .append("COMMENT")
                .append("\n '")
                .append(commentText)
                .append("'\n"));

        sqlDefinition
                .append("AS ")
                .append(SELECT_SOURCE_TABLE_SQL)
                .append(";");

        return sqlDefinition.toString();
    }

    @DataProvider
    public Object[][] getTestMaterializedViewDtos()
    {
        return new Object[][] {
                {
                        ImmutableList.of(),
                        ImmutableList.of()
                },
                {
                        ImmutableList.of(
                                createMaterializedViewDto(FULLY_QUALIFIED_NAME_1, Optional.of(MATERIALIZED_VIEW_COMMENT_1), false)),
                        ImmutableList.of(
                                createMaterializedViewRow(FULLY_QUALIFIED_NAME_1, Optional.of(MATERIALIZED_VIEW_COMMENT_1), false))
                },
                {
                        ImmutableList.of(
                                createMaterializedViewDto(FULLY_QUALIFIED_NAME_1, Optional.of(MATERIALIZED_VIEW_COMMENT_1), false),
                                createMaterializedViewDto(FULLY_QUALIFIED_NAME_2, Optional.of(MATERIALIZED_VIEW_COMMENT_2), true)),
                        ImmutableList.of(
                                createMaterializedViewRow(FULLY_QUALIFIED_NAME_1, Optional.of(MATERIALIZED_VIEW_COMMENT_1), false),
                                createMaterializedViewRow(FULLY_QUALIFIED_NAME_2, Optional.of(MATERIALIZED_VIEW_COMMENT_2), true))
                },
                {
                        ImmutableList.of(
                                createMaterializedViewDto(FULLY_QUALIFIED_NAME_2, Optional.of(EMPTY_COMMENT), true),
                                createMaterializedViewDto(FULLY_QUALIFIED_NAME_2, Optional.empty(), true)),
                        ImmutableList.of(
                                createMaterializedViewRow(FULLY_QUALIFIED_NAME_2, Optional.of(EMPTY_COMMENT), true),
                                createMaterializedViewRow(FULLY_QUALIFIED_NAME_2, Optional.empty(), true))
                },
                {
                        ImmutableList.of(
                                createMaterializedViewDto(FULLY_QUALIFIED_NAME_2, Optional.of(EMPTY_COMMENT), true),
                                createMaterializedViewDto(FULLY_QUALIFIED_NAME_2, Optional.empty(), false)),
                        ImmutableList.of(
                                createMaterializedViewRow(FULLY_QUALIFIED_NAME_2, Optional.of(EMPTY_COMMENT), true),
                                createMaterializedViewRow(FULLY_QUALIFIED_NAME_2, Optional.empty(), false))
                }
        };
    }

    @Test(dataProvider = "getTestMaterializedViewDtos")
    public void toTableRows(List<MaterializedViewDto> materializedViewDtos, List<Object[]> materializedViewRows)
    {
        assertThat(MATERIALIZED_VIEW_ROW_ADAPTER.toTableRows(materializedViewDtos))
                .isEqualToComparingFieldByFieldRecursively(materializedViewRows);
    }
}
