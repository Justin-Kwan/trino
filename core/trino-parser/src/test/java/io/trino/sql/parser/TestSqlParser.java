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
package io.trino.sql.parser;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AllRows;
import io.trino.sql.tree.Analyze;
import io.trino.sql.tree.AnchorPattern;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.CreateRole;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Cube;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.DescribeOutput;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.DropMaterializedView;
import io.trino.sql.tree.DropRole;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.DropView;
import io.trino.sql.tree.EmptyPattern;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainFormat;
import io.trino.sql.tree.ExplainType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FetchFirst;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.FunctionCall.NullTreatment;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Grant;
import io.trino.sql.tree.GrantOnType;
import io.trino.sql.tree.GrantRoles;
import io.trino.sql.tree.GrantorSpecification;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.GroupingSets;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IntervalLiteral.IntervalField;
import io.trino.sql.tree.IntervalLiteral.Sign;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.Isolation;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.Lateral;
import io.trino.sql.tree.LikeClause;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.LogicalBinaryExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Merge;
import io.trino.sql.tree.MergeDelete;
import io.trino.sql.tree.MergeInsert;
import io.trino.sql.tree.MergeUpdate;
import io.trino.sql.tree.NaturalJoin;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OneOrMoreQuantifier;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.PathElement;
import io.trino.sql.tree.PathSpecification;
import io.trino.sql.tree.PatternAlternation;
import io.trino.sql.tree.PatternConcatenation;
import io.trino.sql.tree.PatternVariable;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.PrincipalSpecification;
import io.trino.sql.tree.ProcessingMode;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.QuantifiedPattern;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.RangeQuantifier;
import io.trino.sql.tree.RefreshMaterializedView;
import io.trino.sql.tree.RenameColumn;
import io.trino.sql.tree.RenameSchema;
import io.trino.sql.tree.RenameTable;
import io.trino.sql.tree.RenameView;
import io.trino.sql.tree.ResetSession;
import io.trino.sql.tree.Revoke;
import io.trino.sql.tree.RevokeRoles;
import io.trino.sql.tree.Rollback;
import io.trino.sql.tree.Rollup;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SetPath;
import io.trino.sql.tree.SetRole;
import io.trino.sql.tree.SetSession;
import io.trino.sql.tree.SetTableAuthorization;
import io.trino.sql.tree.SetViewAuthorization;
import io.trino.sql.tree.ShowCatalogs;
import io.trino.sql.tree.ShowColumns;
import io.trino.sql.tree.ShowFunctions;
import io.trino.sql.tree.ShowGrants;
import io.trino.sql.tree.ShowRoleGrants;
import io.trino.sql.tree.ShowRoles;
import io.trino.sql.tree.ShowSchemas;
import io.trino.sql.tree.ShowSession;
import io.trino.sql.tree.ShowStats;
import io.trino.sql.tree.ShowTables;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StartTransaction;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;
import io.trino.sql.tree.TransactionAccessMode;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Update;
import io.trino.sql.tree.UpdateAssignment;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.WindowDefinition;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowReference;
import io.trino.sql.tree.WindowSpecification;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;
import io.trino.sql.tree.ZeroOrMoreQuantifier;
import io.trino.sql.tree.ZeroOrOneQuantifier;
import org.testng.annotations.Test;

import javax.swing.text.html.Option;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.QueryUtil.aliased;
import static io.trino.sql.QueryUtil.ascending;
import static io.trino.sql.QueryUtil.equal;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.sql.QueryUtil.nameReference;
import static io.trino.sql.QueryUtil.ordering;
import static io.trino.sql.QueryUtil.query;
import static io.trino.sql.QueryUtil.quotedIdentifier;
import static io.trino.sql.QueryUtil.row;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.selectListItems;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.simpleQuerySpecification;
import static io.trino.sql.QueryUtil.subquery;
import static io.trino.sql.QueryUtil.table;
import static io.trino.sql.QueryUtil.values;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.parser.ParserAssert.expression;
import static io.trino.sql.parser.ParserAssert.rowPattern;
import static io.trino.sql.parser.ParserAssert.statement;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static io.trino.sql.parser.TreeNodes.columnDefinition;
import static io.trino.sql.parser.TreeNodes.dateTimeType;
import static io.trino.sql.parser.TreeNodes.field;
import static io.trino.sql.parser.TreeNodes.location;
import static io.trino.sql.parser.TreeNodes.property;
import static io.trino.sql.parser.TreeNodes.qualifiedName;
import static io.trino.sql.parser.TreeNodes.rowType;
import static io.trino.sql.parser.TreeNodes.simpleType;
import static io.trino.sql.testing.TreeAssertions.assertFormattedSql;
import static io.trino.sql.tree.ArithmeticUnaryExpression.negative;
import static io.trino.sql.tree.ArithmeticUnaryExpression.positive;
import static io.trino.sql.tree.DateTimeDataType.Type.TIMESTAMP;
import static io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.trino.sql.tree.ProcessingMode.Mode.FINAL;
import static io.trino.sql.tree.ProcessingMode.Mode.RUNNING;
import static io.trino.sql.tree.SortItem.NullOrdering.UNDEFINED;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.sql.tree.SortItem.Ordering.DESCENDING;
import static io.trino.sql.tree.WindowFrame.Type.ROWS;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSqlParser
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testPosition()
    {
        assertThat(expression("position('a' in 'b')"))
                .isEqualTo(new FunctionCall(location(1, 1), QualifiedName.of("strpos"), ImmutableList.of(
                        new StringLiteral(location(1, 17), "b"),
                        new StringLiteral(location(1, 10), "a"))));

        assertThat(expression("position('a' in ('b'))"))
                .isEqualTo(new FunctionCall(location(1, 1), QualifiedName.of("strpos"), ImmutableList.of(
                        new StringLiteral(location(1, 18), "b"),
                        new StringLiteral(location(1, 10), "a"))));
    }

    @Test
    public void testPossibleExponentialBacktracking()
    {
        createExpression("(((((((((((((((((((((((((((true)))))))))))))))))))))))))))");
    }

    @Test(timeOut = 2_000)
    public void testPotentialUnboundedLookahead()
    {
        createExpression("(\n" +
                "      1 * -1 +\n" +
                "      1 * -2 +\n" +
                "      1 * -3 +\n" +
                "      1 * -4 +\n" +
                "      1 * -5 +\n" +
                "      1 * -6 +\n" +
                "      1 * -7 +\n" +
                "      1 * -8 +\n" +
                "      1 * -9 +\n" +
                "      1 * -10 +\n" +
                "      1 * -11 +\n" +
                "      1 * -12 \n" +
                ")\n");
    }

    @Test
    public void testQualifiedName()
    {
        assertEquals(QualifiedName.of("a", "b", "c", "d").toString(), "a.b.c.d");
        assertEquals(QualifiedName.of("A", "b", "C", "d").toString(), "a.b.c.d");
        assertTrue(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("b", "c", "d")));
        assertTrue(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("a", "b", "c", "d")));
        assertFalse(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("a", "c", "d")));
        assertFalse(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("z", "a", "b", "c", "d")));
        assertEquals(QualifiedName.of("a", "b", "c", "d"), QualifiedName.of("a", "b", "c", "d"));
    }

    @Test
    public void testGenericLiteral()
    {
        assertGenericLiteral("VARCHAR");
        assertGenericLiteral("BIGINT");
        assertGenericLiteral("DOUBLE");
        assertGenericLiteral("BOOLEAN");
        assertGenericLiteral("DATE");
        assertGenericLiteral("foo");
    }

    @Test
    public void testBinaryLiteral()
    {
        assertThat(expression("x' '")).isEqualTo(new BinaryLiteral(location(1, 1), ""));
        assertThat(expression("x''")).isEqualTo(new BinaryLiteral(location(1, 1), ""));
        assertThat(expression("X'abcdef1234567890ABCDEF'")).isEqualTo(new BinaryLiteral(location(1, 1), "abcdef1234567890ABCDEF"));

        // forms such as "X 'a b' " may look like BinaryLiteral
        // but they do not pass the syntax rule for BinaryLiteral
        // but instead conform to TypeConstructor, which generates a GenericLiteral expression
        assertInvalidExpression("X 'a b'", "Spaces are not allowed.*");
        assertInvalidExpression("X'a b c'", "Binary literal must contain an even number of digits.*");
        assertInvalidExpression("X'a z'", "Binary literal can only contain hexadecimal digits.*");
    }

    public static void assertGenericLiteral(String type)
    {
        assertExpression(type + " 'abc'", new GenericLiteral(type, "abc"));
    }

    @Test
    public void testLiterals()
    {
        assertThat(expression("TIME 'abc'")).isEqualTo(new TimeLiteral(location(1, 1), "abc"));
        assertThat(expression("TIMESTAMP 'abc'")).isEqualTo(new TimestampLiteral(location(1, 1), "abc"));
        assertThat(expression("INTERVAL '33' day")).isEqualTo(new IntervalLiteral(location(1, 1), "33", Sign.POSITIVE, IntervalField.DAY, Optional.empty()));
        assertThat(expression("INTERVAL '33' day to second")).isEqualTo(new IntervalLiteral(location(1, 1), "33", Sign.POSITIVE, IntervalField.DAY, Optional.of(IntervalField.SECOND)));
        assertThat(expression("CHAR 'abc'")).isEqualTo(new CharLiteral(location(1, 1), "abc"));
    }

    @Test
    public void testNumbers()
    {
        assertThat(expression("9223372036854775807")).isEqualTo(new LongLiteral(location(1, 1), "9223372036854775807"));
        assertThat(expression("-9223372036854775808")).isEqualTo(new LongLiteral(location(1, 1), "-9223372036854775808"));

        assertThat(expression("1E5")).isEqualTo(new DoubleLiteral(location(1, 1), "1E5"));
        assertThat(expression("1E-5")).isEqualTo(new DoubleLiteral(location(1, 1), "1E-5"));
        assertThat(expression(".1E5")).isEqualTo(new DoubleLiteral(location(1, 1), ".1E5"));
        assertThat(expression(".1E-5")).isEqualTo(new DoubleLiteral(location(1, 1), ".1E-5"));
        assertThat(expression("1.1E5")).isEqualTo(new DoubleLiteral(location(1, 1), "1.1E5"));
        assertThat(expression("1.1E-5")).isEqualTo(new DoubleLiteral(location(1, 1), "1.1E-5"));

        assertThat(expression("-1E5")).isEqualTo(new DoubleLiteral(location(1, 1), "-1E5"));
        assertThat(expression("-1E-5")).isEqualTo(new DoubleLiteral(location(1, 1), "-1E-5"));
        assertThat(expression("-.1E5")).isEqualTo(new DoubleLiteral(location(1, 1), "-.1E5"));
        assertThat(expression("-.1E-5")).isEqualTo(new DoubleLiteral(location(1, 1), "-.1E-5"));
        assertThat(expression("-1.1E5")).isEqualTo(new DoubleLiteral(location(1, 1), "-1.1E5"));
        assertThat(expression("-1.1E-5")).isEqualTo(new DoubleLiteral(location(1, 1), "-1.1E-5"));

        assertThat(expression(".1")).isEqualTo(new DecimalLiteral(location(1, 1), ".1"));
        assertThat(expression("1.2")).isEqualTo(new DecimalLiteral(location(1, 1), "1.2"));
        assertThat(expression("-1.2")).isEqualTo(new DecimalLiteral(location(1, 1), "-1.2"));
    }

    @Test
    public void testArrayConstructor()
    {
        assertThat(expression("ARRAY []")).isEqualTo(new ArrayConstructor(location(1, 1), ImmutableList.of()));
        assertThat(expression("ARRAY [1, 2]")).isEqualTo(new ArrayConstructor(location(1, 1), ImmutableList.of(new LongLiteral(location(1, 8), "1"), new LongLiteral(location(1, 11), "2"))));
        assertThat(expression("ARRAY [1e0, 2.5e0]")).isEqualTo(new ArrayConstructor(location(1, 1), ImmutableList.of(new DoubleLiteral(location(1, 8), "1.0"), new DoubleLiteral(location(1, 13), "2.5"))));
        assertThat(expression("ARRAY ['hi']")).isEqualTo(new ArrayConstructor(location(1, 1), ImmutableList.of(new StringLiteral(location(1, 8), "hi"))));
        assertThat(expression("ARRAY ['hi', 'hello']")).isEqualTo(new ArrayConstructor(location(1, 1), ImmutableList.of(new StringLiteral(location(1, 8), "hi"), new StringLiteral(location(1, 14), "hello"))));
    }

    @Test
    public void testArraySubscript()
    {
        assertThat(expression("ARRAY [1, 2][1]")).isEqualTo(new SubscriptExpression(location(1, 1),
                new ArrayConstructor(location(1, 1), ImmutableList.of(new LongLiteral(location(1, 8), "1"), new LongLiteral(location(1, 11), "2"))),
                new LongLiteral(location(1, 14), "1")));

        assertThat(expression("CASE WHEN TRUE THEN ARRAY[1,2] END[1]")).isEqualTo(new SubscriptExpression(location(1, 1),
                new SearchedCaseExpression(location(1, 1),
                        ImmutableList.of(
                                new WhenClause(
                                        location(1, 6),
                                        new BooleanLiteral(location(1, 11), "true"),
                                        new ArrayConstructor(location(1, 21), ImmutableList.of(new LongLiteral(location(1, 27), "1"), new LongLiteral(location(1, 29), "2"))))),
                        Optional.empty()),
                new LongLiteral(location(1, 36), "1")));
    }

    @Test
    public void testRowSubscript()
    {
        assertThat(expression("ROW (1, 'a', true)[1]")).isEqualTo(new SubscriptExpression(location(1, 1),
                new Row(location(1, 1), ImmutableList.of(new LongLiteral(location(1, 6), "1"), new StringLiteral(location(1, 9), "a"), new BooleanLiteral(location(1, 14), "true"))),
                new LongLiteral(location(1, 20), "1")));
    }

    @Test
    public void testAllColumns() {
        assertThat(statement("SELECT r.* FROM t")).isEqualTo(simpleQuery(
                new Select(
                        location(1, 1),
                        false,
                        ImmutableList.of(
                                new AllColumns(
                                        location(1, 8),
                                        Optional.of(new Identifier(location(1, 8), "r", false)),
                                        ImmutableList.of()))),
                table(QualifiedName.of("t"))));

        assertThat(statement("SELECT ROW (1, 'a', true).*")).isEqualTo(simpleQuery(
                new Select(
                        false,
                        ImmutableList.of(
                                new AllColumns(
                                        Optional.empty(),
                                        Optional.of(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("a"), new BooleanLiteral("true")))),
                                        ImmutableList.of())))));

        assertThat(statement("SELECT ROW (1, 'a', true).* AS (f1, f2, f3)")).isEqualTo(simpleQuery(
                new Select(
                        false,
                        ImmutableList.of(
                                new AllColumns(
                                        Optional.empty(),
                                        Optional.of(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("a"), new BooleanLiteral("true")))),
                                        ImmutableList.of(new Identifier("f1"), new Identifier("f2"), new Identifier("f3")))))));
    }

    @Test
    public void testDouble()
    {
        assertThat(expression("123E7")).isEqualTo(new DoubleLiteral(location(1, 1), "123E7"));
        assertThat(expression("123.E7")).isEqualTo(new DoubleLiteral(location(1, 1), "123.E7"));
        assertThat(expression("123.0E7")).isEqualTo(new DoubleLiteral(location(1, 1), "123.0E7"));
        assertThat(expression("123E+7")).isEqualTo(new DoubleLiteral(location(1, 1), "123E+7"));
        assertThat(expression("123E-7")).isEqualTo(new DoubleLiteral(location(1, 1), "123E-7"));

        assertThat(expression("123.456E7")).isEqualTo(new DoubleLiteral(location(1, 1), "123.456E7"));
        assertThat(expression("123.456E+7")).isEqualTo(new DoubleLiteral(location(1, 1), "123.456E+7"));
        assertThat(expression("123.456E-7")).isEqualTo(new DoubleLiteral(location(1, 1), "123.456E-7"));

        assertThat(expression(".4E42")).isEqualTo(new DoubleLiteral(location(1, 1), ".4E42"));
        assertThat(expression(".4E+42")).isEqualTo(new DoubleLiteral(location(1, 1), ".4E+42"));
        assertThat(expression(".4E-42")).isEqualTo(new DoubleLiteral(location(1, 1), ".4E-42"));
    }

    @Test
    public void testArithmeticUnary()
    {
        assertThat(expression("9")).isEqualTo(new LongLiteral(location(1, 1), "9"));

        assertThat(expression("+9")).isEqualTo(positive(location(1, 1), new LongLiteral(location(1, 2), "9")));
        assertThat(expression("+ 9")).isEqualTo(positive(location(1, 1), new LongLiteral(location(1, 3), "9")));

        assertThat(expression("++9")).isEqualTo(positive(location(1, 1), positive(location(1, 2), new LongLiteral(location(1, 3), "9"))));
        assertThat(expression("+ +9")).isEqualTo(positive(location(1, 1), positive(location(1, 3), new LongLiteral(location(1, 4), "9"))));
        assertThat(expression("+ + 9")).isEqualTo(positive(location(1, 1), positive(location(1, 3), new LongLiteral(location(1, 5), "9"))));

        assertThat(expression("+++9")).isEqualTo(positive(location(1, 1), positive(location(1, 2), positive(location(1, 3), new LongLiteral(location(1, 4), "9")))));
        assertThat(expression("+ + +9")).isEqualTo(positive(location(1, 1), positive(location(1, 3), positive(location(1, 5), new LongLiteral(location(1, 6), "9")))));
        assertThat(expression("+ + + 9")).isEqualTo(positive(location(1, 1), positive(location(1, 3), positive(location(1, 5), new LongLiteral(location(1, 7), "9")))));

        assertThat(expression("-9")).isEqualTo(new LongLiteral(location(1, 1), "-9"));
        assertThat(expression("- 9")).isEqualTo(new LongLiteral(location(1, 1), "-9"));

        assertThat(expression("- + 9")).isEqualTo(negative(location(1, 1), positive(location(1, 3), new LongLiteral(location(1, 5), "9"))));
        assertThat(expression("-+9")).isEqualTo(negative(location(1, 1), positive(location(1, 2), new LongLiteral(location(1, 3), "9"))));

        assertThat(expression("+ - + 9")).isEqualTo(positive(location(1, 1), negative(location(1, 3), positive(location(1, 5), new LongLiteral(location(1, 7), "9")))));
        assertThat(expression("+-+9")).isEqualTo(positive(location(1, 1), negative(location(1, 2), positive(location(1, 3), new LongLiteral(location(1, 4), "9")))));

        assertThat(expression("- -9")).isEqualTo(negative(location(1, 1), new LongLiteral(location(1, 3), "-9")));
        assertThat(expression("- - 9")).isEqualTo(negative(location(1, 1), new LongLiteral(location(1, 3), "-9")));

        assertThat(expression("- + - + 9")).isEqualTo(negative(location(1, 1), positive(location(1, 3), negative(location(1, 5), positive(location(1, 7), new LongLiteral(location(1, 9), "9"))))));
        assertThat(expression("-+-+9")).isEqualTo(negative(location(1, 1), positive(location(1, 2), negative(location(1, 3), positive(location(1, 4), new LongLiteral(location(1, 5), "9"))))));

        assertThat(expression("+ - + - + 9")).isEqualTo(positive(location(1, 1), negative(location(1, 3), positive(location(1, 5), negative(location(1, 7), positive(location(1, 9), new LongLiteral(location(1, 11), "9")))))));
        assertThat(expression("+-+-+9")).isEqualTo(positive(location(1, 1), negative(location(1, 2), positive(location(1, 3), negative(location(1, 4), positive(location(1, 5), new LongLiteral(location(1, 6), "9")))))));

        assertThat(expression("- - -9")).isEqualTo(negative(location(1, 1), negative(location(1, 3), new LongLiteral(location(1, 5), "-9"))));
        assertThat(expression("- - - 9")).isEqualTo(negative(location(1, 1), negative(location(1, 3), new LongLiteral(location(1, 5), "-9"))));
    }

    @Test
    public void testCoalesce()
    {
        assertInvalidExpression("coalesce()", "The 'coalesce' function must have at least two arguments");
        assertInvalidExpression("coalesce(5)", "The 'coalesce' function must have at least two arguments");
        assertInvalidExpression("coalesce(1, 2) filter (where true)", "FILTER not valid for 'coalesce' function");
        assertInvalidExpression("coalesce(1, 2) OVER ()", "OVER clause not valid for 'coalesce' function");
        assertThat(expression("coalesce(13, 42)")).isEqualTo(new CoalesceExpression(location(1, 1), ImmutableList.of(new LongLiteral(location(1, 10), "13"), new LongLiteral(location(1, 14), "42"))));
        assertThat(expression("coalesce(6, 7, 8)")).isEqualTo(new CoalesceExpression(location(1, 1), ImmutableList.of(new LongLiteral(location(1, 10), "6"), new LongLiteral(location(1, 13), "7"), new LongLiteral(location(1, 16), "8"))));
        assertThat(expression("coalesce(13, null)")).isEqualTo(new CoalesceExpression(location(1, 1), ImmutableList.of(new LongLiteral(location(1, 10), "13"), new NullLiteral(location(1, 14)))));
        assertThat(expression("coalesce(null, 13)")).isEqualTo(new CoalesceExpression(location(1, 1), ImmutableList.of(new NullLiteral(location(1, 10)), new LongLiteral(location(1, 16), "13"))));
        assertThat(expression("coalesce(null, null)")).isEqualTo(new CoalesceExpression(location(1, 1), ImmutableList.of(new NullLiteral(location(1, 10)), new NullLiteral(location(1, 16)))));
    }

    @Test
    public void testIf()
    {
        assertThat(expression("if(true, 1, 0)")).isEqualTo(new IfExpression(location(1, 1), new BooleanLiteral(location(1, 4), "true"), new LongLiteral(location(1, 10), "1"), new LongLiteral(location(1, 13), "0")));
        assertThat(expression("if(true, 3, null)")).isEqualTo(new IfExpression(location(1, 1), new BooleanLiteral(location(1, 4), "true"), new LongLiteral(location(1, 10), "3"), new NullLiteral(location(1, 13))));
        assertThat(expression("if(false, null, 4)")).isEqualTo(new IfExpression(location(1, 1), new BooleanLiteral(location(1, 4), "false"), new NullLiteral(location(1, 11)), new LongLiteral(location(1, 17), "4")));
        assertThat(expression("if(false, null, null)")).isEqualTo(new IfExpression(location(1, 1), new BooleanLiteral(location(1, 4), "false"), new NullLiteral(location(1, 11)), new NullLiteral(location(1, 17))));
        assertThat(expression("if(true, 3)")).isEqualTo(new IfExpression(location(1, 1), new BooleanLiteral(location(1, 4), "true"), new LongLiteral(location(1, 10), "3"), null));
        assertInvalidExpression("IF(true)", "Invalid number of arguments for 'if' function");
        assertInvalidExpression("IF(true, 1, 0) FILTER (WHERE true)", "FILTER not valid for 'if' function");
        assertInvalidExpression("IF(true, 1, 0) OVER()", "OVER clause not valid for 'if' function");
    }

    @Test
    public void testNullIf()
    {
        assertThat(expression("nullif(42, 87)")).isEqualTo(new NullIfExpression(location(1, 1), new LongLiteral(location(1, 8), "42"), new LongLiteral(location(1, 12), "87")));
        assertThat(expression("nullif(42, null)")).isEqualTo(new NullIfExpression(location(1, 1), new LongLiteral(location(1, 8), "42"), new NullLiteral(location(1, 12))));
        assertThat(expression("nullif(null, null)")).isEqualTo(new NullIfExpression(location(1, 1), new NullLiteral(location(1, 8)), new NullLiteral(location(1, 14))));
        assertInvalidExpression("nullif(1)", "Invalid number of arguments for 'nullif' function");
        assertInvalidExpression("nullif(1, 2, 3)", "Invalid number of arguments for 'nullif' function");
        assertInvalidExpression("nullif(42, 87) filter (where true)", "FILTER not valid for 'nullif' function");
        assertInvalidExpression("nullif(42, 87) OVER ()", "OVER clause not valid for 'nullif' function");
    }

    @Test
    public void testDoubleInQuery()
    {
        assertThat(statement("SELECT 123.456E7 FROM DUAL")).isEqualTo(
                simpleQuery(
                        location(1, 1),
                        new Select(
                                location(1, 1),
                                false,
                                selectListItems(new DoubleLiteral(location(1, 8), "123.456E7"))),
                        new Table(
                                location(1, 23),
                                qualifiedName(location(1, 23), "DUAL"))));
    }

    @Test
    public void testIntersect()
    {
        assertThat(statement("SELECT 123 INTERSECT DISTINCT SELECT 123 INTERSECT ALL SELECT 123")).isEqualTo(
                query(
                        location(1, 1),
                        new Intersect(
                                location(1, 42),
                                ImmutableList.of(
                                        new Intersect(
                                                location(1, 12),
                                                ImmutableList.of(
                                                        createSelect123(location(1, 1), location(1, 8)),
                                                        createSelect123(location(1, 31), location(1, 38))),
                                                true),
                                        createSelect123(location(1, 56), location(1, 63))),
                        false)));
    }

    @Test
    public void testUnion()
    {
        assertThat(statement("SELECT 123 UNION DISTINCT SELECT 123 UNION ALL SELECT 123")).isEqualTo(
                query(
                        location(1, 1),
                        new Union(
                                location(1, 38),
                                ImmutableList.of(
                                        new Union(
                                                location(1, 12),
                                                ImmutableList.of(
                                                        createSelect123(location(1, 1), location(1, 8)),
                                                        createSelect123(location(1, 27), location(1, 34))),
                                                true),
                                createSelect123(location(1, 48), location(1, 55))),
                        false)));
    }

    public static QuerySpecification createSelect123(NodeLocation queryLocation, NodeLocation columnLocation)
    {
        return simpleQuerySpecification(
                queryLocation,
                selectList(queryLocation, new LongLiteral(columnLocation, "123")));
    }

    @Test
    public void testReservedWordIdentifier()
    {
        assertThat(statement("SELECT id FROM public.orders")).isEqualTo(
                simpleQuery(
                        location(1, 1),
                        selectList(location(1, 1), new Identifier(location(1, 8), "id", false)),
                        new Table(location(1, 16),
                                QualifiedName.of(ImmutableList.of(
                                        new Identifier(location(1, 16), "public", false),
                                        new Identifier(location(1, 23), "orders", false))))));

        assertThat(statement("SELECT id FROM \"public\".\"order\"")).isEqualTo(
                simpleQuery(
                        location(1, 1),
                        selectList(location(1, 1), new Identifier(location(1, 8), "id", false)),
                        new Table(location(1, 16),
                                QualifiedName.of(ImmutableList.of(
                                new Identifier(location(1, 16), "public", true),
                                new Identifier(location(1, 25), "order", true))))));

        assertThat(statement("SELECT id FROM \"public\".\"order\"\"2\"")).isEqualTo(
                simpleQuery(
                        location(1, 1),
                        selectList(location(1, 1), new Identifier(location(1, 8), "id", false)),
                        new Table(location(1, 16), QualifiedName.of(ImmutableList.of(
                                new Identifier(location(1,16), "public", true),
                                new Identifier(location(1,25), "order\"2", true))))));
    }

    @Test
    public void testBetween()
    {
        assertThat(expression("1 BETWEEN 2 AND 3"))
                .isEqualTo(new BetweenPredicate(location(1, 3),
                        new LongLiteral(location(1, 1), "1"),
                        new LongLiteral(location(1, 11), "2"),
                        new LongLiteral(location(1, 17), "3")));

        assertThat(expression("1 NOT BETWEEN 2 AND 3"))
                .isEqualTo(new NotExpression(location(1, 3),
                        new BetweenPredicate(location(1, 3),
                                new LongLiteral(location(1, 1), "1"),
                                new LongLiteral(location(1, 15), "2"),
                                new LongLiteral(location(1, 21), "3"))));
    }

    @Test
    public void testSelectWithLimit()
    {
        final Select selectClause = selectList(
                location(1, 1),
                new AllColumns(location(1, 8), Optional.empty(), ImmutableList.of()));

        final Table table = new Table(
                location(1, 15),
                QualifiedName.of(ImmutableList.of(new Identifier(location(1,15),"table1", false))));

        assertThat(statement("SELECT * FROM table1 LIMIT 2")).isEqualTo(
                simpleQuery(
                        location(1, 1),
                        selectClause,
                        table,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Limit(location(1, 22), new LongLiteral(location(1, 28), "2")))));

        assertThat(statement("SELECT * FROM table1 LIMIT ALL")).isEqualTo(
                simpleQuery(
                        location(1, 1),
                        selectClause,
                        table,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Limit(location(1, 22), new AllRows(location(1, 28))))));

        final Query valuesQuery = query(location(1, 16), values(location(1, 16),
                row(location(1, 23), new LongLiteral(location(1, 24), "1"), new StringLiteral(location(1, 27), "1")),
                row(location(1, 33), new LongLiteral(location(1, 34), "2"), new StringLiteral(location(1, 37), "2"))));

        assertThat(statement("SELECT * FROM (VALUES (1, '1'), (2, '2')) LIMIT ALL")).isEqualTo(
                simpleQuery(location(1, 1),
                        selectList(location(1, 1), new AllColumns(location(1, 8), Optional.empty(), ImmutableList.of())),
                        subquery(location(1, 15), valuesQuery),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Limit(location(1, 43), new AllRows(location(1, 49))))));
    }

    @Test
    public void testValues()
    {
        final Query valuesQuery = query(location(1, 1), values(location(1, 1),
                row(
                        location(1, 8),
                        new StringLiteral(location(1, 9), "a"),
                        new LongLiteral(location(1, 14), "1"),
                        new DoubleLiteral(location(1, 17), "2.2")),
                row(
                        location(1, 25),
                        new StringLiteral(location(1, 26), "b"),
                        new LongLiteral(location(1, 31), "2"),
                        new DoubleLiteral(location(1, 34),"3.3"))));

        assertThat(statement("VALUES ('a', 1, 2.2e0), ('b', 2, 3.3e0)")).isEqualTo(valuesQuery);

        final Query valuesSubquery = query(location(1, 16), values(location(1, 16),
                row(
                        location(1, 23),
                        new StringLiteral(location(1, 24), "a"),
                        new LongLiteral(location(1, 29), "1"),
                        new DoubleLiteral(location(1, 32), "2.2")),
                row(
                        location(1, 40),
                        new StringLiteral(location(1, 41), "b"),
                        new LongLiteral(location(1, 46), "2"),
                        new DoubleLiteral(location(1, 49),"3.3"))));

        assertThat(statement("SELECT * FROM (VALUES ('a', 1, 2.2e0), ('b', 2, 3.3e0))")).isEqualTo(
                simpleQuery(
                        location(1, 1),
                        selectList(location(1, 1), new AllColumns(location(1, 8), Optional.empty(), ImmutableList.of())),
                        subquery(location(1, 15), valuesSubquery)));
    }

    @Test
    public void testRowPattern()
    {
        assertThat(rowPattern("(A B)* | CC+? DD?? E | (F | G)"))
                .isEqualTo(
                        new PatternAlternation(
                                location(1, 1),
                                ImmutableList.of(
                                        new PatternAlternation(
                                                location(1, 1),
                                                ImmutableList.of(
                                                        new QuantifiedPattern(
                                                                location(1, 1),
                                                                new PatternConcatenation(
                                                                        location(1, 2),
                                                                        ImmutableList.of(
                                                                                new PatternVariable(location(1, 2), new Identifier(location(1, 2), "A", false)),
                                                                                new PatternVariable(location(1, 4), new Identifier(location(1, 4), "B", false)))),
                                                                new ZeroOrMoreQuantifier(location(1, 6), true)),
                                                        new PatternConcatenation(
                                                                location(1, 10),
                                                                ImmutableList.of(
                                                                        new PatternConcatenation(
                                                                                location(1, 10),
                                                                                ImmutableList.of(
                                                                                        new QuantifiedPattern(location(1, 10), new PatternVariable(location(1, 10), new Identifier(location(1, 10), "CC", false)), new OneOrMoreQuantifier(location(1, 12), false)),
                                                                                        new QuantifiedPattern(location(1, 15), new PatternVariable(location(1, 15), new Identifier(location(1, 15), "DD", false)), new ZeroOrOneQuantifier(location(1, 17), false)))),
                                                                        new PatternVariable(location(1, 20), new Identifier(location(1, 20), "E", false)))))),
                                        new PatternAlternation(
                                                location(1, 25),
                                                ImmutableList.of(
                                                        new PatternVariable(location(1, 25), new Identifier(location(1, 25), "F", false)),
                                                        new PatternVariable(location(1, 29), new Identifier(location(1, 29), "G", false)))))));

        assertThat(rowPattern("A | B | C D E F"))
                .isEqualTo(
                        new PatternAlternation(
                                location(1, 1),
                                ImmutableList.of(
                                        new PatternAlternation(
                                                location(1, 1),
                                                ImmutableList.of(
                                                        new PatternVariable(location(1, 1), new Identifier(location(1, 1), "A", false)),
                                                        new PatternVariable(location(1, 5), new Identifier(location(1, 5), "B", false)))),
                                        new PatternConcatenation(
                                                location(1, 9),
                                                ImmutableList.of(
                                                        new PatternConcatenation(
                                                                location(1, 9),
                                                                ImmutableList.of(
                                                                        new PatternConcatenation(
                                                                                location(1, 9),
                                                                                ImmutableList.of(
                                                                                        new PatternVariable(location(1, 9), new Identifier(location(1, 9), "C", false)),
                                                                                        new PatternVariable(location(1, 11), new Identifier(location(1, 11), "D", false)))),
                                                                        new PatternVariable(location(1, 13), new Identifier(location(1, 13), "E", false)))),
                                                        new PatternVariable(location(1, 15), new Identifier(location(1, 15), "F", false)))))));

        assertThatThrownBy(() -> SQL_PARSER.createRowPattern("A!"))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line 1:2: mismatched input '!'.*");

        assertThatThrownBy(() -> SQL_PARSER.createRowPattern("A**"))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line 1:3: mismatched input '*'.*");

        assertThat(rowPattern("A??"))
                .isEqualTo(new QuantifiedPattern(
                        location(1, 1),
                        new PatternVariable(location(1, 1), new Identifier(location(1, 1), "A", false)),
                        new ZeroOrOneQuantifier(location(1, 2), false)));

        assertThat(rowPattern("^$"))
                .isEqualTo(new PatternConcatenation(
                        location(1, 1),
                        ImmutableList.of(
                                new AnchorPattern(location(1, 1), AnchorPattern.Type.PARTITION_START),
                                new AnchorPattern(location(1, 2), AnchorPattern.Type.PARTITION_END))));

        assertThat(rowPattern("()"))
                .isEqualTo(new EmptyPattern(location(1, 1)));

        assertThat(rowPattern("A{3}"))
                .isEqualTo(new QuantifiedPattern(
                        location(1, 1),
                        new PatternVariable(location(1, 1), new Identifier(location(1, 1), "A", false)),
                        new RangeQuantifier(location(1, 2), true, Optional.of(new LongLiteral(location(1, 3), "3")), Optional.of(new LongLiteral(location(1, 3), "3")))));

        assertThat(rowPattern("A{3,}"))
                .isEqualTo(new QuantifiedPattern(
                        location(1, 1),
                        new PatternVariable(location(1, 1), new Identifier(location(1, 1), "A", false)),
                        new RangeQuantifier(location(1, 2), true, Optional.of(new LongLiteral(location(1, 3), "3")), Optional.empty())));

        assertThat(rowPattern("A{,3}"))
                .isEqualTo(new QuantifiedPattern(
                        location(1, 1),
                        new PatternVariable(location(1, 1), new Identifier(location(1, 1), "A", false)),
                        new RangeQuantifier(location(1, 2), true, Optional.empty(), Optional.of(new LongLiteral(location(1, 4), "3")))));

        assertThat(rowPattern("A{3,4}"))
                .isEqualTo(new QuantifiedPattern(
                        location(1, 1),
                        new PatternVariable(location(1, 1), new Identifier(location(1, 1), "A", false)),
                        new RangeQuantifier(location(1, 2), true, Optional.of(new LongLiteral(location(1, 3), "3")), Optional.of(new LongLiteral(location(1, 5), "4")))));
    }

    @Test
    public void testPrecedenceAndAssociativity()
    {
        assertThat(expression("1 AND 2 OR 3")).isEqualTo(
                new LogicalBinaryExpression(location(1, 9), LogicalBinaryExpression.Operator.OR,
                    new LogicalBinaryExpression(location(1, 3), LogicalBinaryExpression.Operator.AND,
                        new LongLiteral(location(1, 1), "1"),
                        new LongLiteral(location(1, 7), "2")),
                        new LongLiteral(location(1, 12), "3")));

        assertThat(expression("1 OR 2 AND 3")).isEqualTo(new LogicalBinaryExpression(location(1, 3), LogicalBinaryExpression.Operator.OR,
                new LongLiteral(location(1, 1), "1"),
                new LogicalBinaryExpression(location(1, 8), LogicalBinaryExpression.Operator.AND,
                        new LongLiteral(location(1, 6), "2"),
                        new LongLiteral(location(1, 12), "3"))));

        assertThat(expression("NOT 1 AND 2")).isEqualTo(new LogicalBinaryExpression(location(1, 7), LogicalBinaryExpression.Operator.AND,
                new NotExpression(location(1, 1), new LongLiteral(location(1, 5), "1")),
                new LongLiteral(location(1, 11), "2")));

        assertThat(expression("NOT 1 OR 2")).isEqualTo(new LogicalBinaryExpression(location(1, 7), LogicalBinaryExpression.Operator.OR,
                new NotExpression(location(1, 1), new LongLiteral(location(1, 5), "1")),
                new LongLiteral(location(1, 10), "2")));

        assertThat(expression("-1 + 2")).isEqualTo(new ArithmeticBinaryExpression(location(1, 4), ArithmeticBinaryExpression.Operator.ADD,
                new LongLiteral(location(1, 1), "-1"),
                new LongLiteral(location(1, 6), "2")));

        assertThat(expression("1 - 2 - 3")).isEqualTo(new ArithmeticBinaryExpression(location(1, 7), ArithmeticBinaryExpression.Operator.SUBTRACT,
                new ArithmeticBinaryExpression(location(1, 3), ArithmeticBinaryExpression.Operator.SUBTRACT,
                        new LongLiteral(location(1, 1), "1"),
                        new LongLiteral(location(1, 5), "2")),
                new LongLiteral(location(1, 9), "3")));

        assertThat(expression("1 / 2 / 3")).isEqualTo(new ArithmeticBinaryExpression(location(1, 7), ArithmeticBinaryExpression.Operator.DIVIDE,
                new ArithmeticBinaryExpression(location(1, 3), ArithmeticBinaryExpression.Operator.DIVIDE,
                        new LongLiteral(location(1, 1), "1"),
                        new LongLiteral(location(1, 5), "2")),
                new LongLiteral(location(1, 9), "3")));

        assertThat(expression("1 + 2 * 3")).isEqualTo(new ArithmeticBinaryExpression(location(1, 3), ArithmeticBinaryExpression.Operator.ADD,
                new LongLiteral(location(1, 1), "1"),
                new ArithmeticBinaryExpression(location(1, 7), ArithmeticBinaryExpression.Operator.MULTIPLY,
                        new LongLiteral(location(1, 5), "2"),
                        new LongLiteral(location(1, 9), "3"))));
    }

    @Test
    public void testInterval()
    {
        assertThat(expression("INTERVAL '123' YEAR")).isEqualTo(new IntervalLiteral(location(1, 1), "123", Sign.POSITIVE, IntervalField.YEAR, Optional.empty()));
        assertThat(expression("INTERVAL '123-3' YEAR TO MONTH")).isEqualTo(new IntervalLiteral(location(1, 1), "123-3", Sign.POSITIVE, IntervalField.YEAR, Optional.of(IntervalField.MONTH)));
        assertThat(expression("INTERVAL '123' MONTH")).isEqualTo(new IntervalLiteral(location(1, 1), "123", Sign.POSITIVE, IntervalField.MONTH, Optional.empty()));
        assertThat(expression("INTERVAL '123' DAY")).isEqualTo(new IntervalLiteral(location(1, 1), "123", Sign.POSITIVE, IntervalField.DAY, Optional.empty()));
        assertThat(expression("INTERVAL '123 23:58:53.456' DAY TO SECOND")).isEqualTo(new IntervalLiteral(location(1, 1), "123 23:58:53.456", Sign.POSITIVE, IntervalField.DAY, Optional.of(IntervalField.SECOND)));
        assertThat(expression("INTERVAL '123' HOUR")).isEqualTo(new IntervalLiteral(location(1, 1), "123", Sign.POSITIVE, IntervalField.HOUR, Optional.empty()));
        assertThat(expression("INTERVAL '23:59' HOUR TO MINUTE")).isEqualTo(new IntervalLiteral(location(1, 1), "23:59", Sign.POSITIVE, IntervalField.HOUR, Optional.of(IntervalField.MINUTE)));
        assertThat(expression("INTERVAL '123' MINUTE")).isEqualTo(new IntervalLiteral(location(1, 1), "123", Sign.POSITIVE, IntervalField.MINUTE, Optional.empty()));
        assertThat(expression("INTERVAL '123' SECOND")).isEqualTo(new IntervalLiteral(location(1, 1), "123", Sign.POSITIVE, IntervalField.SECOND, Optional.empty()));
    }

    @Test
    public void testDecimal()
    {
        assertThat(expression("DECIMAL '12.34'")).isEqualTo(new DecimalLiteral(location(1, 1), "12.34"));
        assertThat(expression("DECIMAL '12.'")).isEqualTo(new DecimalLiteral(location(1, 1), "12."));
        assertThat(expression("DECIMAL '12'")).isEqualTo(new DecimalLiteral(location(1, 1), "12"));
        assertThat(expression("DECIMAL '.34'")).isEqualTo(new DecimalLiteral(location(1, 1), ".34"));
        assertThat(expression("DECIMAL '+12.34'")).isEqualTo(new DecimalLiteral(location(1, 1), "+12.34"));
        assertThat(expression("DECIMAL '+12'")).isEqualTo(new DecimalLiteral(location(1, 1), "+12"));
        assertThat(expression("DECIMAL '-12.34'")).isEqualTo(new DecimalLiteral(location(1, 1), "-12.34"));
        assertThat(expression("DECIMAL '-12'")).isEqualTo(new DecimalLiteral(location(1, 1), "-12"));
        assertThat(expression("DECIMAL '+.34'")).isEqualTo(new DecimalLiteral(location(1, 1), "+.34"));
        assertThat(expression("DECIMAL '-.34'")).isEqualTo(new DecimalLiteral(location(1, 1), "-.34"));

        assertInvalidExpression("123.", "Unexpected decimal literal: 123.");
        assertInvalidExpression("123.0", "Unexpected decimal literal: 123.0");
        assertInvalidExpression(".5", "Unexpected decimal literal: .5");
        assertInvalidExpression("123.5", "Unexpected decimal literal: 123.5");
    }

    @Test
    public void testTime()
    {
        assertThat(expression("TIME '03:04:05'")).isEqualTo(new TimeLiteral(location(1, 1), "03:04:05"));
    }

    @Test
    public void testCurrentTimestamp()
    {
        assertThat(expression("CURRENT_TIMESTAMP")).isEqualTo(new CurrentTime(location(1, 1), CurrentTime.Function.TIMESTAMP));
    }

    @Test
    public void testFormat()
    {
        assertThat(expression("format('%s', 'abc')")).isEqualTo((new Format(location(1, 1), ImmutableList.of(new StringLiteral(location(1, 8), "%s"), new StringLiteral(location(1, 14), "abc")))));
        assertThat(expression("format('%d %s', 123, 'x')")).isEqualTo((new Format(location(1, 1), ImmutableList.of(new StringLiteral(location(1, 8), "%d %s"), new LongLiteral(location(1, 17), "123"), new StringLiteral(location(1, 22), "x")))));

        assertInvalidExpression("format()", "The 'format' function must have at least two arguments");
        assertInvalidExpression("format('%s')", "The 'format' function must have at least two arguments");
    }

    @Test
    public void testCase()
    {
        assertThat(expression("CASE 1 IS NULL WHEN true THEN 2 ELSE 3 END")).isEqualTo(
                new SimpleCaseExpression(
                        location(1, 1),
                        new IsNullPredicate(
                                location(1, 8),
                                new LongLiteral(location(1, 6), "1")),
                                ImmutableList.of(
                                        new WhenClause(
                                                location(1, 16),
                                                new BooleanLiteral(location(1, 21), "true"),
                                                new LongLiteral(location(1, 31), "2"))),
                                Optional.of(new LongLiteral(location(1, 38), "3"))));
    }

    @Test
    public void testSearchedCase()
    {
        assertThat(expression("CASE WHEN a > 3 THEN 23 WHEN b = a THEN 33 END")).isEqualTo(
                new SearchedCaseExpression(
                        location(1, 1),
                        ImmutableList.of(
                                new WhenClause(
                                        location(1, 6),
                                        new ComparisonExpression(location(1, 13), ComparisonExpression.Operator.GREATER_THAN, new Identifier(location(1, 11), "a", false), new LongLiteral(location(1, 15), "3")),
                                        new LongLiteral(location(1, 22), "23")),
                                new WhenClause(
                                        location(1, 25),
                                        new ComparisonExpression(location(1, 32), ComparisonExpression.Operator.EQUAL, new Identifier(location(1, 30),  "b", false), new Identifier(location(1, 34), "a", false)),
                                        new LongLiteral(location(1, 41), "33"))),
                        Optional.empty()));
    }

    @Test
    public void testSetSession() {
        assertThat(statement("SET SESSION foo = 'bar'")).isEqualTo(new SetSession(location(1, 1), QualifiedName.of(ImmutableList.of(new Identifier(location(1, 13), "foo", false))), new StringLiteral(location(1, 19), "bar")));
        assertThat(statement("SET SESSION foo.bar = 'baz'")).isEqualTo(new SetSession(location(1, 1), QualifiedName.of(ImmutableList.of(new Identifier(location(1, 13), "foo", false), new Identifier(location(1, 17), "bar", false))), new StringLiteral(location(1, 23), "baz")));
        assertThat(statement("SET SESSION foo.bar.boo = 'baz'")).isEqualTo(new SetSession(location(1, 1), QualifiedName.of(ImmutableList.of(new Identifier(location(1, 13), "foo", false), new Identifier(location(1, 17), "bar", false), new Identifier(location(1, 21), "boo", false))), new StringLiteral(location(1, 27), "baz")));

        assertThat(statement("SET SESSION foo.bar = 'ban' || 'ana'")).isEqualTo(new SetSession(
                location(1, 1),
                QualifiedName.of(ImmutableList.of(new Identifier(location(1, 13), "foo", false), new Identifier(location(1, 17), "bar", false))),
                new FunctionCall(location(1, 321), QualifiedName.of("concat"), ImmutableList.of(
                        new StringLiteral(location(1, 23), "ban"),
                        new StringLiteral(location(1, 32), "ana")))));
    }

    @Test
    public void testResetSession()
    {
        assertThat(statement("RESET SESSION foo.bar")).isEqualTo(new ResetSession(location(1, 1), QualifiedName.of(ImmutableList.of(new Identifier(location(1, 15), "foo", false), new Identifier(location(1, 19), "bar", false)))));
        assertThat(statement("RESET SESSION foo")).isEqualTo(new ResetSession(location(1, 1), QualifiedName.of(ImmutableList.of(new Identifier(location(1, 15), "foo", false)))));
    }

    @Test
    public void testShowSession()
    {
        assertThat(statement("SHOW SESSION")).isEqualTo(new ShowSession(location(1, 1), Optional.empty(), Optional.empty()));
        assertThat(statement("SHOW SESSION LIKE '%'")).isEqualTo(new ShowSession(location(1, 1), Optional.of("%"), Optional.empty()));
        assertThat(statement("SHOW SESSION LIKE '%' ESCAPE '$'")).isEqualTo(new ShowSession(location(1, 1), Optional.of("%"), Optional.of("$")));
    }

    @Test
    public void testShowCatalogs()
    {
        assertThat(statement("SHOW CATALOGS")).isEqualTo(new ShowCatalogs(location(1, 1), Optional.empty(), Optional.empty()));
        assertThat(statement("SHOW CATALOGS LIKE '%'")).isEqualTo(new ShowCatalogs(location(1, 1), Optional.of("%"), Optional.empty()));
        assertThat(statement("SHOW CATALOGS LIKE '%$_%' ESCAPE '$'")).isEqualTo(new ShowCatalogs(location(1, 1), Optional.of("%$_%"), Optional.of("$")));
    }

    @Test
    public void testShowSchemas()
    {
        assertThat(statement("SHOW SCHEMAS")).isEqualTo(new ShowSchemas(location(1, 1), Optional.empty(), Optional.empty(), Optional.empty()));
        assertThat(statement("SHOW SCHEMAS FROM foo")).isEqualTo(new ShowSchemas(location(1, 1), Optional.of(new Identifier(location(1, 19), "foo", false)), Optional.empty(), Optional.empty()));
        assertThat(statement("SHOW SCHEMAS IN foo LIKE '%'")).isEqualTo(new ShowSchemas(location(1, 1), Optional.of(new Identifier(location(1, 17), "foo", false)), Optional.of("%"), Optional.empty()));
        assertThat(statement("SHOW SCHEMAS IN foo LIKE '%$_%' ESCAPE '$'")).isEqualTo(new ShowSchemas(location(1, 1), Optional.of(new Identifier(location(1, 17), "foo", false)), Optional.of("%$_%"), Optional.of("$")));
    }

    @Test
    public void testShowTables()
    {
        assertThat(statement("SHOW TABLES")).isEqualTo(new ShowTables(location(1, 1), Optional.empty(), Optional.empty(), Optional.empty()));
        assertThat(statement("SHOW TABLES FROM a")).isEqualTo(new ShowTables(location(1, 1), Optional.of(QualifiedName.of(ImmutableList.of(new Identifier(location(1, 18), "a", false)))), Optional.empty(), Optional.empty()));
        assertThat(statement("SHOW TABLES FROM \"awesome schema\"")).isEqualTo(new ShowTables(location(1, 1), Optional.of(QualifiedName.of(ImmutableList.of(new Identifier(location(1, 18), "awesome schema", true)))), Optional.empty(), Optional.empty()));
        assertThat(statement("SHOW TABLES IN a LIKE '%$_%' ESCAPE '$'")).isEqualTo(new ShowTables(location(1, 1), Optional.of(QualifiedName.of(ImmutableList.of(new Identifier(location(1, 16), "a", false)))), Optional.of("%$_%"), Optional.of("$")));
    }

    @Test
    public void testShowColumns()
    {
        assertThat(statement("SHOW COLUMNS FROM a")).isEqualTo(new ShowColumns(location(1, 1), QualifiedName.of(ImmutableList.of(new Identifier(location(1, 19), "a", false))), Optional.empty(), Optional.empty()));
        assertThat(statement("SHOW COLUMNS FROM a.b")).isEqualTo(new ShowColumns(location(1, 1), QualifiedName.of(ImmutableList.of(new Identifier(location(1, 19), "a", false), new Identifier(location(1, 21), "b", false))), Optional.empty(), Optional.empty()));
        assertThat(statement("SHOW COLUMNS FROM \"awesome table\"")).isEqualTo(new ShowColumns(location(1, 1), QualifiedName.of(ImmutableList.of(new Identifier(location(1, 19), "awesome table", true))), Optional.empty(), Optional.empty()));
        assertThat(statement("SHOW COLUMNS FROM \"awesome schema\".\"awesome table\"")).isEqualTo(new ShowColumns(location(1, 1), QualifiedName.of(ImmutableList.of(new Identifier(location(1, 19), "awesome schema", true), new Identifier(location(1, 36), "awesome table", true))), Optional.empty(), Optional.empty()));
        assertThat(statement("SHOW COLUMNS FROM a.b LIKE '%$_%' ESCAPE '$'")).isEqualTo(new ShowColumns(location(1, 1), QualifiedName.of(ImmutableList.of(new Identifier(location(1, 19), "a", false), new Identifier(location(1, 21), "b", false))), Optional.of("%$_%"), Optional.of("$")));
        assertInvalidStatement("SHOW COLUMNS FROM a.b LIKE null", "mismatched input 'null'. Expecting: <string>");
        assertInvalidStatement("SHOW COLUMNS FROM a.b LIKE 'a' ESCAPE null'", "mismatched input 'null'. Expecting: <string>");
    }

    @Test
    public void testShowFunctions()
    {
        assertThat(statement("SHOW FUNCTIONS")).isEqualTo(new ShowFunctions(location(1, 1), Optional.empty(), Optional.empty()));
        assertThat(statement("SHOW FUNCTIONS LIKE '%'")).isEqualTo(new ShowFunctions(location(1, 1), Optional.of("%"), Optional.empty()));
        assertThat(statement("SHOW FUNCTIONS LIKE '%' ESCAPE '$'")).isEqualTo(new ShowFunctions(location(1, 1), Optional.of("%"), Optional.of("$")));
    }

    @Test
    public void testSubstringBuiltInFunction()
    {
        final String givenString = "ABCDEF";
        assertThat(statement(format("SELECT substring('%s' FROM 2)", givenString))).isEqualTo(
                simpleQuery(
                        location(1, 1),
                        selectList(
                                location(1, 1),
                                new FunctionCall(
                                        location(1, 8),
                                        QualifiedName.of("substr"),
                                        Lists.newArrayList(new StringLiteral(location(1, 18), givenString), new LongLiteral(location(1, 32), "2"))))));

        assertThat(statement(format("SELECT substring('%s' FROM 2 FOR 3)", givenString))).isEqualTo(
                simpleQuery(
                        location(1, 1),
                        selectList(
                                location(1, 1),
                                new FunctionCall(
                                        location(1, 8),
                                        QualifiedName.of("substr"),
                                        Lists.newArrayList(new StringLiteral(location(1, 18), givenString),  new LongLiteral(location(1, 32), "2"),  new LongLiteral(location(1, 38), "3"))))));
    }

    @Test
    public void testSubstringRegisteredFunction()
    {
        final String givenString = "ABCDEF";

        final QualifiedName substringFunctionName = QualifiedName.of(ImmutableList.of(
                new Identifier(location(1, 8), "substring", false)));

        final ArrayList<Expression> substringFunctionArguments = Lists.newArrayList(
                new StringLiteral(location(1, 18), givenString),
                new LongLiteral(location(1, 28), "2"));

        final Query selectSubstringQuery = simpleQuery(location(1, 1),
                selectList(location(1, 1),
                        new FunctionCall(location(1, 8), substringFunctionName, substringFunctionArguments)));

        assertThat(statement(format("SELECT substring('%s', 2)", givenString))).isEqualTo(selectSubstringQuery);

        substringFunctionArguments.add(new LongLiteral(location(1, 31), "3"));

        assertThat(statement(format("SELECT substring('%s', 2, 3)", givenString))).isEqualTo(selectSubstringQuery);
    }

    @Test
    public void testSelectWithRowType() {
        assertThat(statement("SELECT col1.f1, col2, col3.f1.f2.f3 FROM table1")).isEqualTo(simpleQuery(
                selectList(
                        new DereferenceExpression(new Identifier("col1"), identifier("f1")),
                        new Identifier("col2"),
                        new DereferenceExpression(
                                new DereferenceExpression(new DereferenceExpression(new Identifier("col3"), identifier("f1")), identifier("f2")), identifier("f3"))),
                new Table(QualifiedName.of("table1"))));

        assertThat(statement("SELECT col1.f1[0], col2, col3[2].f2.f3, col4[4] FROM table1")).isEqualTo(simpleQuery(
                selectList(
                        new SubscriptExpression(new DereferenceExpression(new Identifier("col1"), identifier("f1")), new LongLiteral("0")),
                        new Identifier("col2"),
                        new DereferenceExpression(new DereferenceExpression(new SubscriptExpression(new Identifier("col3"), new LongLiteral("2")), identifier("f2")), identifier("f3")),
                        new SubscriptExpression(new Identifier("col4"), new LongLiteral("4"))),
                new Table(QualifiedName.of("table1"))));

        assertThat(statement("SELECT CAST(ROW(11, 12) AS ROW(COL0 INTEGER, COL1 INTEGER)).col0")).isEqualTo(simpleQuery(
                selectList(
                        new DereferenceExpression(
                                new Cast(
                                        new Row(Lists.newArrayList(new LongLiteral("11"), new LongLiteral("12"))),
                                        rowType(location(1, 26),
                                                field(location(1, 30), "COL0", simpleType(location(1, 35), "INTEGER")),
                                                field(location(1, 44), "COL1", simpleType(location(1, 49), "INTEGER")))),
                                identifier("col0")))));
    }

    @Test
    public void testSelectWithOrderBy() {
        assertThat(statement("SELECT * FROM table1 ORDER BY a")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Table(QualifiedName.of("table1")),
                ordering(ascending("a"))));
    }

    @Test
    public void testSelectWithOffset() {
        assertThat(statement("SELECT * FROM table1 OFFSET 2 ROWS")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Table(QualifiedName.of("table1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new Offset(new LongLiteral("2"))),
                Optional.empty()));

        assertThat(statement("SELECT * FROM table1 OFFSET 2")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Table(QualifiedName.of("table1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new Offset(new LongLiteral("2"))),
                Optional.empty()));

        Query valuesQuery = query(values(
                row(new LongLiteral("1"), new StringLiteral("1")),
                row(new LongLiteral("2"), new StringLiteral("2"))));

        assertThat(statement("SELECT * FROM (VALUES (1, '1'), (2, '2')) OFFSET 2 ROWS")).isEqualTo(simpleQuery(selectList(new AllColumns()),
                subquery(valuesQuery),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new Offset(new LongLiteral("2"))),
                Optional.empty()));

        assertThat(statement("SELECT * FROM (VALUES (1, '1'), (2, '2')) OFFSET 2")).isEqualTo(simpleQuery(selectList(new AllColumns()),
                subquery(valuesQuery),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new Offset(new LongLiteral("2"))),
                Optional.empty()));
    }

    @Test
    public void testSelectWithFetch() {
        assertThat(statement("SELECT * FROM table1 FETCH FIRST 2 ROWS ONLY")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Table(QualifiedName.of("table1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new FetchFirst(new LongLiteral("2")))));

        assertThat(statement("SELECT * FROM table1 FETCH NEXT ROW ONLY")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Table(QualifiedName.of("table1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new FetchFirst(Optional.empty()))));

        Query valuesQuery = query(values(
                row(new LongLiteral("1"), new StringLiteral("1")),
                row(new LongLiteral("2"), new StringLiteral("2"))));

        assertThat(statement("SELECT * FROM (VALUES (1, '1'), (2, '2')) FETCH FIRST ROW ONLY")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                subquery(valuesQuery),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new FetchFirst(Optional.empty()))));

        assertThat(statement("SELECT * FROM (VALUES (1, '1'), (2, '2')) FETCH FIRST ROW WITH TIES")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                subquery(valuesQuery),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new FetchFirst(Optional.empty(), true))));

        assertThat(statement("SELECT * FROM table1 FETCH FIRST 2 ROWS WITH TIES")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Table(QualifiedName.of("table1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new FetchFirst(new LongLiteral("2"), true))));

        assertThat(statement("SELECT * FROM table1 FETCH NEXT ROW WITH TIES")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Table(QualifiedName.of("table1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new FetchFirst(Optional.empty(), true))));
    }

    @Test
    public void testSelectWithGroupBy() {
        assertThat(statement("SELECT * FROM table1 GROUP BY a")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Table(QualifiedName.of("table1")),
                Optional.empty(),
                Optional.of(new GroupBy(false, ImmutableList.of(new SimpleGroupBy(ImmutableList.of(new Identifier("a")))))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));

        assertThat(statement("SELECT * FROM table1 GROUP BY a, b")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Table(QualifiedName.of("table1")),
                Optional.empty(),
                Optional.of(new GroupBy(false, ImmutableList.of(
                        new SimpleGroupBy(ImmutableList.of(new Identifier("a"))),
                        new SimpleGroupBy(ImmutableList.of(new Identifier("b")))))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));

        assertThat(statement("SELECT * FROM table1 GROUP BY ()")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Table(QualifiedName.of("table1")),
                Optional.empty(),
                Optional.of(new GroupBy(false, ImmutableList.of(new SimpleGroupBy(ImmutableList.of())))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));

        assertThat(statement("SELECT * FROM table1 GROUP BY GROUPING SETS (a)")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Table(QualifiedName.of("table1")),
                Optional.empty(),
                Optional.of(new GroupBy(false, ImmutableList.of(new GroupingSets(
                        ImmutableList.of(
                                ImmutableList.of(new Identifier("a"))))))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));

        assertThat(statement("SELECT a, b, GROUPING(a, b) FROM table1 GROUP BY GROUPING SETS ((a), (b))")).isEqualTo(simpleQuery(
                selectList(
                        DereferenceExpression.from(QualifiedName.of("a")),
                        DereferenceExpression.from(QualifiedName.of("b")),
                        new GroupingOperation(
                                Optional.empty(),
                                ImmutableList.of(QualifiedName.of("a"), QualifiedName.of("b")))),
                new Table(QualifiedName.of("table1")),
                Optional.empty(),
                Optional.of(new GroupBy(false, ImmutableList.of(new GroupingSets(
                        ImmutableList.of(
                                ImmutableList.of(new Identifier("a")),
                                ImmutableList.of(new Identifier("b"))))))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));

        assertThat(statement("SELECT * FROM table1 GROUP BY ALL GROUPING SETS ((a, b), (a), ()), CUBE (c), ROLLUP (d)")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Table(QualifiedName.of("table1")),
                Optional.empty(),
                Optional.of(new GroupBy(false, ImmutableList.of(
                        new GroupingSets(
                                ImmutableList.of(
                                        ImmutableList.of(new Identifier("a"), new Identifier("b")),
                                        ImmutableList.of(new Identifier("a")),
                                        ImmutableList.of())),
                        new Cube(ImmutableList.of(new Identifier("c"))),
                        new Rollup(ImmutableList.of(new Identifier("d")))))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));

        assertThat(statement("SELECT * FROM table1 GROUP BY DISTINCT GROUPING SETS ((a, b), (a), ()), CUBE (c), ROLLUP (d)")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Table(QualifiedName.of("table1")),
                Optional.empty(),
                Optional.of(new GroupBy(true, ImmutableList.of(
                        new GroupingSets(
                                ImmutableList.of(
                                        ImmutableList.of(new Identifier("a"), new Identifier("b")),
                                        ImmutableList.of(new Identifier("a")),
                                        ImmutableList.of())),
                        new Cube(ImmutableList.of(new Identifier("c"))),
                        new Rollup(ImmutableList.of(new Identifier("d")))))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));
    }

    @Test
    public void testCreateSchema() {
        assertThat(statement("CREATE SCHEMA test")).isEqualTo(new CreateSchema(QualifiedName.of("test"), false, ImmutableList.of()));

        assertThat(statement("CREATE SCHEMA IF NOT EXISTS test")).isEqualTo(new CreateSchema(QualifiedName.of("test"), true, ImmutableList.of()));

        assertThat(statement("CREATE SCHEMA test WITH (a = 'apple', b = 123)")).isEqualTo(new CreateSchema(
                QualifiedName.of("test"),
                false,
                ImmutableList.of(
                        new Property(new Identifier("a"), new StringLiteral("apple")),
                        new Property(new Identifier("b"), new LongLiteral("123")))));

        assertThat(statement("CREATE SCHEMA \"some name that contains space\"")).isEqualTo(new CreateSchema(QualifiedName.of("some name that contains space"), false, ImmutableList.of()));
    }

    @Test
    public void testDropSchema() {
        assertThat(statement("DROP SCHEMA test")).isEqualTo(new DropSchema(QualifiedName.of("test"), false, false));

        assertThat(statement("DROP SCHEMA test CASCADE")).isEqualTo(new DropSchema(QualifiedName.of("test"), false, true));

        assertThat(statement("DROP SCHEMA IF EXISTS test")).isEqualTo(new DropSchema(QualifiedName.of("test"), true, false));

        assertThat(statement("DROP SCHEMA IF EXISTS test RESTRICT")).isEqualTo(new DropSchema(QualifiedName.of("test"), true, false));

        assertThat(statement("DROP SCHEMA \"some schema that contains space\"")).isEqualTo(new DropSchema(QualifiedName.of("some schema that contains space"), false, false));
    }

    @Test
    public void testRenameSchema() {
        assertThat(statement("ALTER SCHEMA foo RENAME TO bar")).isEqualTo(new RenameSchema(QualifiedName.of("foo"), identifier("bar")));

        assertThat(statement("ALTER SCHEMA foo.bar RENAME TO baz")).isEqualTo(new RenameSchema(QualifiedName.of("foo", "bar"), identifier("baz")));

        assertThat(statement("ALTER SCHEMA \"awesome schema\".\"awesome table\" RENAME TO \"even more awesome table\"")).isEqualTo(new RenameSchema(QualifiedName.of("awesome schema", "awesome table"), quotedIdentifier("even more awesome table")));
    }

    @Test
    public void testUnicodeString()
    {
        assertExpression("U&''", new StringLiteral(""));
        assertExpression("U&'' UESCAPE ')'", new StringLiteral(""));
        assertExpression("U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801'", new StringLiteral("hello\u6d4B\u8Bd5\uDBFF\uDFFFworld\u7F16\u7801"));
        assertExpression("U&'\u6d4B\u8Bd5ABC\\6d4B\\8Bd5'", new StringLiteral("\u6d4B\u8Bd5ABC\u6d4B\u8Bd5"));
        assertExpression("u&'\u6d4B\u8Bd5ABC\\6d4B\\8Bd5'", new StringLiteral("\u6d4B\u8Bd5ABC\u6d4B\u8Bd5"));
        assertExpression("u&'\u6d4B\u8Bd5ABC\\\\'", new StringLiteral("\u6d4B\u8Bd5ABC\\"));
        assertExpression("u&'\u6d4B\u8Bd5ABC###8Bd5' UESCAPE '#'", new StringLiteral("\u6d4B\u8Bd5ABC#\u8Bd5"));
        assertExpression("u&'\u6d4B\u8Bd5''A''B''C##''''#8Bd5' UESCAPE '#'", new StringLiteral("\u6d4B\u8Bd5\'A\'B\'C#\'\'\u8Bd5"));
        assertInvalidExpression("U&  '\u6d4B\u8Bd5ABC\\\\'", ".*mismatched input.*");
        assertInvalidExpression("u&'\u6d4B\u8Bd5ABC\\'", "Incomplete escape sequence: ");
        assertInvalidExpression("u&'\u6d4B\u8Bd5ABC\\+'", "Incomplete escape sequence: ");
        assertInvalidExpression("U&'hello\\6dB\\8Bd5'", "Incomplete escape sequence: 6dB.*");
        assertInvalidExpression("U&'hello\\6D4B\\8Bd'", "Incomplete escape sequence: 8Bd");
        assertInvalidExpression("U&'hello\\K6B\\8Bd5'", "Invalid hexadecimal digit: K");
        assertInvalidExpression("U&'hello\\+FFFFFD\\8Bd5'", "Invalid escaped character: FFFFFD");
        assertInvalidExpression("U&'hello\\DBFF'", "Invalid escaped character: DBFF\\. Escaped character is a surrogate\\. Use \'\\\\\\+123456\' instead\\.");
        assertInvalidExpression("U&'hello\\+00DBFF'", "Invalid escaped character: 00DBFF\\. Escaped character is a surrogate\\. Use \'\\\\\\+123456\' instead\\.");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '%%'", "Invalid Unicode escape character: %%");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '\uDBFF'", "Invalid Unicode escape character: \uDBFF");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '\n'", "Invalid Unicode escape character: \n");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE ''''", "Invalid Unicode escape character: \'");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE ' '", "Invalid Unicode escape character:  ");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE ''", "Empty Unicode escape character");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '1'", "Invalid Unicode escape character: 1");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '+'", "Invalid Unicode escape character: \\+");
        assertExpression("U&'hello!6d4B!8Bd5!+10FFFFworld!7F16!7801' UESCAPE '!'", new StringLiteral("hello\u6d4B\u8Bd5\uDBFF\uDFFFworld\u7F16\u7801"));
        assertExpression("U&'\u6d4B\u8Bd5ABC!6d4B!8Bd5' UESCAPE '!'", new StringLiteral("\u6d4B\u8Bd5ABC\u6d4B\u8Bd5"));
        assertExpression("U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801' UESCAPE '!'",
                new StringLiteral("hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801"));
    }

    @Test
    public void testCreateTable() {
        assertThat(statement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c IPADDRESS)"))
                .isEqualTo(new CreateTable(
                        location(1, 1),
                        qualifiedName(location(1, 14), "foo"),
                        ImmutableList.of(
                                columnDefinition(location(1, 19), "a", simpleType(location(1, 21), "VARCHAR")),
                                columnDefinition(location(1, 30), "b", simpleType(location(1, 32), "BIGINT"), true, "hello world"),
                                columnDefinition(location(1, 62), "c", simpleType(location(1, 64), "IPADDRESS"))),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS bar (c TIMESTAMP)"))
                .isEqualTo(new CreateTable(
                        location(1, 1),
                        qualifiedName(location(1, 28), "bar"),
                        ImmutableList.of(
                                columnDefinition(location(1, 33), "c", dateTimeType(location(1, 35), TIMESTAMP, false), true)),
                        true,
                        ImmutableList.of(),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS bar (c VARCHAR WITH (nullable = true, compression = 'LZ4'))"))
                .describedAs("CREATE TABLE with column properties")
                .isEqualTo(
                        new CreateTable(
                                location(1, 1),
                                qualifiedName(location(1, 28), "bar"),
                                ImmutableList.of(
                                        columnDefinition(
                                                location(1, 33),
                                                "c",
                                                simpleType(location(1, 35), "VARCHAR"),
                                                true,
                                                ImmutableList.of(
                                                        property(location(1, 49), "nullable", new BooleanLiteral(location(1, 60), "true")),
                                                        property(location(1, 66), "compression", new StringLiteral(location(1, 80), "LZ4"))))),
                                true,
                                ImmutableList.of(),
                                Optional.empty()));

        // with LIKE
        assertThat(statement("CREATE TABLE IF NOT EXISTS bar (LIKE like_table)")).isEqualTo(new CreateTable(QualifiedName.of("bar"),
                ImmutableList.of(
                        new LikeClause(QualifiedName.of("like_table"),
                                Optional.empty())),
                true,
                ImmutableList.of(),
                Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS bar (c VARCHAR, LIKE like_table)"))
                .ignoringLocation()
                .isEqualTo(new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("c"), simpleType(location(1, 35), "VARCHAR"), true, emptyList(), Optional.empty()),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.empty())),
                        true,
                        ImmutableList.of(),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS bar (c VARCHAR, LIKE like_table, d BIGINT)"))
                .ignoringLocation()
                .isEqualTo(new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("c"), simpleType(location(1, 35), "VARCHAR"), true, emptyList(), Optional.empty()),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.empty()),
                                new ColumnDefinition(identifier("d"), simpleType(location(1, 63), "BIGINT"), true, emptyList(), Optional.empty())),
                        true,
                        ImmutableList.of(),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS bar (LIKE like_table INCLUDING PROPERTIES)")).isEqualTo(new CreateTable(QualifiedName.of("bar"),
                ImmutableList.of(
                        new LikeClause(QualifiedName.of("like_table"),
                                Optional.of(LikeClause.PropertiesOption.INCLUDING))),
                true,
                ImmutableList.of(),
                Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS bar (c VARCHAR, LIKE like_table EXCLUDING PROPERTIES)"))
                .ignoringLocation()
                .isEqualTo(new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("c"), simpleType(location(1, 35), "VARCHAR"), true, emptyList(), Optional.empty()),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.of(LikeClause.PropertiesOption.EXCLUDING))),
                        true,
                        ImmutableList.of(),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS bar (c VARCHAR, LIKE like_table EXCLUDING PROPERTIES) COMMENT 'test'"))
                .ignoringLocation()
                .isEqualTo(new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("c"), simpleType(location(1, 35), "VARCHAR"), true, emptyList(), Optional.empty()),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.of(LikeClause.PropertiesOption.EXCLUDING))),
                        true,
                        ImmutableList.of(),
                        Optional.of("test")));
    }

    @Test
    public void testCreateTableWithNotNull()
    {
        assertThat(statement(
                "CREATE TABLE foo (" +
                        "a VARCHAR NOT NULL COMMENT 'column a', " +
                        "b BIGINT COMMENT 'hello world', " +
                        "c IPADDRESS, " +
                        "d INTEGER NOT NULL)"))
                .ignoringLocation()
                .isEqualTo(new CreateTable(
                        QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(identifier("a"), simpleType(location(1, 20), "VARCHAR"), false, emptyList(), Optional.of("column a")),
                                new ColumnDefinition(identifier("b"), simpleType(location(1, 59), "BIGINT"), true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(identifier("c"), simpleType(location(1, 91), "IPADDRESS"), true, emptyList(), Optional.empty()),
                                new ColumnDefinition(identifier("d"), simpleType(location(1, 104), "INTEGER"), false, emptyList(), Optional.empty())),
                        false,
                        ImmutableList.of(),
                        Optional.empty()));
    }

    @Test
    public void testCreateTableAsSelect() {
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t")));
        Query querySelectColumn = simpleQuery(selectList(new Identifier("a")), table(QualifiedName.of("t")));
        Query querySelectColumns = simpleQuery(selectList(new Identifier("a"), new Identifier("b")), table(QualifiedName.of("t")));
        QualifiedName table = QualifiedName.of("foo");

        assertThat(statement("CREATE TABLE foo AS SELECT * FROM t")).isEqualTo(new CreateTableAsSelect(table, query, false, ImmutableList.of(), true, Optional.empty(), Optional.empty()));
        assertThat(statement("CREATE TABLE foo(x) AS SELECT a FROM t")).isEqualTo(new CreateTableAsSelect(table, querySelectColumn, false, ImmutableList.of(), true, Optional.of(ImmutableList.of(new Identifier("x"))), Optional.empty()));
        assertThat(statement("CREATE TABLE foo(x,y) AS SELECT a,b FROM t")).isEqualTo(new CreateTableAsSelect(table, querySelectColumns, false, ImmutableList.of(), true, Optional.of(ImmutableList.of(new Identifier("x"), new Identifier("y"))), Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS foo AS SELECT * FROM t")).isEqualTo(new CreateTableAsSelect(table, query, true, ImmutableList.of(), true, Optional.empty(), Optional.empty()));
        assertThat(statement("CREATE TABLE IF NOT EXISTS foo(x) AS SELECT a FROM t")).isEqualTo(new CreateTableAsSelect(table, querySelectColumn, true, ImmutableList.of(), true, Optional.of(ImmutableList.of(new Identifier("x"))), Optional.empty()));
        assertThat(statement("CREATE TABLE IF NOT EXISTS foo(x,y) AS SELECT a,b FROM t")).isEqualTo(new CreateTableAsSelect(table, querySelectColumns, true, ImmutableList.of(), true, Optional.of(ImmutableList.of(new Identifier("x"), new Identifier("y"))), Optional.empty()));

        assertThat(statement("CREATE TABLE foo AS SELECT * FROM t WITH NO DATA")).isEqualTo(new CreateTableAsSelect(table, query, false, ImmutableList.of(), false, Optional.empty(), Optional.empty()));
        assertThat(statement("CREATE TABLE foo(x) AS SELECT a FROM t WITH NO DATA")).isEqualTo(new CreateTableAsSelect(table, querySelectColumn, false, ImmutableList.of(), false, Optional.of(ImmutableList.of(new Identifier("x"))), Optional.empty()));
        assertThat(statement("CREATE TABLE foo(x,y) AS SELECT a,b FROM t WITH NO DATA")).isEqualTo(new CreateTableAsSelect(table, querySelectColumns, false, ImmutableList.of(), false, Optional.of(ImmutableList.of(new Identifier("x"), new Identifier("y"))), Optional.empty()));

        List<Property> properties = ImmutableList.of(
                new Property(new Identifier("string"), new StringLiteral("bar")),
                new Property(new Identifier("long"), new LongLiteral("42")),
                new Property(
                        new Identifier("computed"),
                        new FunctionCall(QualifiedName.of("concat"), ImmutableList.of(new StringLiteral("ban"), new StringLiteral("ana")))),
                new Property(new Identifier("a"), new ArrayConstructor(ImmutableList.of(new StringLiteral("v1"), new StringLiteral("v2")))));

        assertThat(statement("CREATE TABLE foo " +
                "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                "AS " +
                "SELECT * FROM t")).isEqualTo(new CreateTableAsSelect(table, query, false, properties, true, Optional.empty(), Optional.empty()));
        assertThat(statement("CREATE TABLE foo(x) " +
                "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                "AS " +
                "SELECT a FROM t")).isEqualTo(new CreateTableAsSelect(table, querySelectColumn, false, properties, true, Optional.of(ImmutableList.of(new Identifier("x"))), Optional.empty()));
        assertThat(statement("CREATE TABLE foo(x,y) " +
                "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                "AS " +
                "SELECT a,b FROM t")).isEqualTo(new CreateTableAsSelect(table, querySelectColumns, false, properties, true, Optional.of(ImmutableList.of(new Identifier("x"), new Identifier("y"))), Optional.empty()));

        assertThat(statement("CREATE TABLE foo " +
                "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                "AS " +
                "SELECT * FROM t " +
                "WITH NO DATA")).isEqualTo(new CreateTableAsSelect(table, query, false, properties, false, Optional.empty(), Optional.empty()));
        assertThat(statement("CREATE TABLE foo(x) " +
                "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                "AS " +
                "SELECT a FROM t " +
                "WITH NO DATA")).isEqualTo(new CreateTableAsSelect(table, querySelectColumn, false, properties, false, Optional.of(ImmutableList.of(new Identifier("x"))), Optional.empty()));
        assertThat(statement("CREATE TABLE foo(x,y) " +
                "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                "AS " +
                "SELECT a,b FROM t " +
                "WITH NO DATA")).isEqualTo(new CreateTableAsSelect(table, querySelectColumns, false, properties, false, Optional.of(ImmutableList.of(new Identifier("x"), new Identifier("y"))), Optional.empty()));

        assertThat(statement("CREATE TABLE foo COMMENT 'test'" +
                "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                "AS " +
                "SELECT * FROM t " +
                "WITH NO DATA")).isEqualTo(new CreateTableAsSelect(table, query, false, properties, false, Optional.empty(), Optional.of("test")));
        assertThat(statement("CREATE TABLE foo(x) COMMENT 'test'" +
                "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                "AS " +
                "SELECT a FROM t " +
                "WITH NO DATA")).isEqualTo(new CreateTableAsSelect(table, querySelectColumn, false, properties, false, Optional.of(ImmutableList.of(new Identifier("x"))), Optional.of("test")));
        assertThat(statement("CREATE TABLE foo(x,y) COMMENT 'test'" +
                "WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] ) " +
                "AS " +
                "SELECT a,b FROM t " +
                "WITH NO DATA")).isEqualTo(new CreateTableAsSelect(table, querySelectColumns, false, properties, false, Optional.of(ImmutableList.of(new Identifier("x"), new Identifier("y"))), Optional.of("test")));
        assertThat(statement("CREATE TABLE foo(x,y) COMMENT 'test'" +
                "WITH ( \"string\" = 'bar', \"long\" = 42, computed = 'ban' || 'ana', a = ARRAY[ 'v1', 'v2' ] ) " +
                "AS " +
                "SELECT a,b FROM t " +
                "WITH NO DATA")).isEqualTo(new CreateTableAsSelect(table, querySelectColumns, false, properties, false, Optional.of(ImmutableList.of(new Identifier("x"), new Identifier("y"))), Optional.of("test")));
    }

    @Test
    public void testCreateTableAsWith() {
        String queryParenthesizedWith = "CREATE TABLE foo " +
                "AS " +
                "( WITH t(x) AS (VALUES 1) " +
                "TABLE t ) " +
                "WITH NO DATA";
        String queryUnparenthesizedWith = "CREATE TABLE foo " +
                "AS " +
                "WITH t(x) AS (VALUES 1) " +
                "TABLE t " +
                "WITH NO DATA";
        String queryParenthesizedWithHasAlias = "CREATE TABLE foo(a) " +
                "AS " +
                "( WITH t(x) AS (VALUES 1) " +
                "TABLE t ) " +
                "WITH NO DATA";
        String queryUnparenthesizedWithHasAlias = "CREATE TABLE foo(a) " +
                "AS " +
                "WITH t(x) AS (VALUES 1) " +
                "TABLE t " +
                "WITH NO DATA";

        QualifiedName table = QualifiedName.of("foo");

        Query query = new Query(
                Optional.of(new With(false, ImmutableList.of(
                        new WithQuery(
                                identifier("t"),
                                query(new Values(ImmutableList.of(new LongLiteral("1")))),
                                Optional.of(ImmutableList.of(identifier("x"))))))),
                new Table(QualifiedName.of("t")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        assertThat(statement(queryParenthesizedWith)).isEqualTo(new CreateTableAsSelect(table, query, false, ImmutableList.of(), false, Optional.empty(), Optional.empty()));
        assertThat(statement(queryUnparenthesizedWith)).isEqualTo(new CreateTableAsSelect(table, query, false, ImmutableList.of(), false, Optional.empty(), Optional.empty()));
        assertThat(statement(queryParenthesizedWithHasAlias)).isEqualTo(new CreateTableAsSelect(table, query, false, ImmutableList.of(), false, Optional.of(ImmutableList.of(new Identifier("a"))), Optional.empty()));
        assertThat(statement(queryUnparenthesizedWithHasAlias)).isEqualTo(new CreateTableAsSelect(table, query, false, ImmutableList.of(), false, Optional.of(ImmutableList.of(new Identifier("a"))), Optional.empty()));
    }

    @Test
    public void testDropTable() {
        assertThat(statement("DROP TABLE a")).isEqualTo(new DropTable(QualifiedName.of("a"), false));
        assertThat(statement("DROP TABLE a.b")).isEqualTo(new DropTable(QualifiedName.of("a", "b"), false));
        assertThat(statement("DROP TABLE a.b.c")).isEqualTo(new DropTable(QualifiedName.of("a", "b", "c"), false));

        assertThat(statement("DROP TABLE IF EXISTS a")).isEqualTo(new DropTable(QualifiedName.of("a"), true));
        assertThat(statement("DROP TABLE IF EXISTS a.b")).isEqualTo(new DropTable(QualifiedName.of("a", "b"), true));
        assertThat(statement("DROP TABLE IF EXISTS a.b.c")).isEqualTo(new DropTable(QualifiedName.of("a", "b", "c"), true));
    }

    @Test
    public void testDropView() {
        assertThat(statement("DROP VIEW a")).isEqualTo(new DropView(QualifiedName.of("a"), false));
        assertThat(statement("DROP VIEW a.b")).isEqualTo(new DropView(QualifiedName.of("a", "b"), false));
        assertThat(statement("DROP VIEW a.b.c")).isEqualTo(new DropView(QualifiedName.of("a", "b", "c"), false));

        assertThat(statement("DROP VIEW IF EXISTS a")).isEqualTo(new DropView(QualifiedName.of("a"), true));
        assertThat(statement("DROP VIEW IF EXISTS a.b")).isEqualTo(new DropView(QualifiedName.of("a", "b"), true));
        assertThat(statement("DROP VIEW IF EXISTS a.b.c")).isEqualTo(new DropView(QualifiedName.of("a", "b", "c"), true));
    }

    @Test
    public void testInsertInto() {
        QualifiedName table = QualifiedName.of("a");
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t")));

        assertThat(statement("INSERT INTO a SELECT * FROM t")).isEqualTo(new Insert(table, Optional.empty(), query));

        assertThat(statement("INSERT INTO a (c1, c2) SELECT * FROM t")).isEqualTo(new Insert(table, Optional.of(ImmutableList.of(identifier("c1"), identifier("c2"))), query));
    }

    @Test
    public void testDelete() {
        assertThat(statement("DELETE FROM t")).isEqualTo(new Delete(table(QualifiedName.of("t")), Optional.empty()));
        assertThat(statement("DELETE FROM \"awesome table\"")).isEqualTo(new Delete(table(QualifiedName.of("awesome table")), Optional.empty()));

        assertThat(statement("DELETE FROM t WHERE a = b")).isEqualTo(new Delete(table(QualifiedName.of("t")), Optional.of(
                new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                        new Identifier("a"),
                        new Identifier("b")))));
    }

    @Test
    public void testMerge() {
        assertThat(statement("" +
                "MERGE INTO inventory AS i " +
                "  USING changes AS c " +
                "  ON i.part = c.part " +
                "WHEN MATCHED AND c.action = 'mod' " +
                "  THEN UPDATE SET " +
                "    qty = qty + c.qty " +
                "  , ts = CURRENT_TIMESTAMP " +
                "WHEN MATCHED AND c.action = 'del' " +
                "  THEN DELETE " +
                "WHEN NOT MATCHED AND c.action = 'new' " +
                "  THEN INSERT (part, qty) VALUES (c.part, c.qty)")).isEqualTo(new Merge(
                table(QualifiedName.of("inventory")),
                Optional.of(new Identifier("i")),
                aliased(table(QualifiedName.of("changes")), "c"),
                equal(nameReference("i", "part"), nameReference("c", "part")),
                ImmutableList.of(
                        new MergeUpdate(
                                Optional.of(equal(nameReference("c", "action"), new StringLiteral("mod"))),
                                ImmutableList.of(
                                        new MergeUpdate.Assignment(new Identifier("qty"), new ArithmeticBinaryExpression(
                                                ArithmeticBinaryExpression.Operator.ADD,
                                                nameReference("qty"),
                                                nameReference("c", "qty"))),
                                        new MergeUpdate.Assignment(new Identifier("ts"), new CurrentTime(CurrentTime.Function.TIMESTAMP)))),
                        new MergeDelete(
                                Optional.of(equal(nameReference("c", "action"), new StringLiteral("del")))),
                        new MergeInsert(
                                Optional.of(equal(nameReference("c", "action"), new StringLiteral("new"))),
                                ImmutableList.of(new Identifier("part"), new Identifier("qty")),
                                ImmutableList.of(nameReference("c", "part"), nameReference("c", "qty"))))));
    }

    @Test
    public void testRenameTable() {
        assertThat(statement("ALTER TABLE a RENAME TO b")).isEqualTo(new RenameTable(QualifiedName.of("a"), QualifiedName.of("b"), false));
        assertThat(statement("ALTER TABLE IF EXISTS a RENAME TO b")).isEqualTo(new RenameTable(QualifiedName.of("a"), QualifiedName.of("b"), true));
    }

    @Test
    public void testCommentTable() {
        assertThat(statement("COMMENT ON TABLE a IS 'test'")).isEqualTo(new Comment(Comment.Type.TABLE, QualifiedName.of("a"), Optional.of("test")));
        assertThat(statement("COMMENT ON TABLE a IS ''")).isEqualTo(new Comment(Comment.Type.TABLE, QualifiedName.of("a"), Optional.of("")));
        assertThat(statement("COMMENT ON TABLE a IS NULL")).isEqualTo(new Comment(Comment.Type.TABLE, QualifiedName.of("a"), Optional.empty()));
    }

    @Test
    public void testCommentColumn() {
        assertThat(statement("COMMENT ON COLUMN a.b IS 'test'")).isEqualTo(new Comment(Comment.Type.COLUMN, QualifiedName.of("a", "b"), Optional.of("test")));
        assertThat(statement("COMMENT ON COLUMN a.b IS ''")).isEqualTo(new Comment(Comment.Type.COLUMN, QualifiedName.of("a", "b"), Optional.of("")));
        assertThat(statement("COMMENT ON COLUMN a.b IS NULL")).isEqualTo(new Comment(Comment.Type.COLUMN, QualifiedName.of("a", "b"), Optional.empty()));

        assertThat(statement("COMMENT ON COLUMN a IS 'test'")).isEqualTo(new Comment(Comment.Type.COLUMN, QualifiedName.of("a"), Optional.of("test")));
        assertThat(statement("COMMENT ON COLUMN a.b.c IS 'test'")).isEqualTo(new Comment(Comment.Type.COLUMN, QualifiedName.of("a", "b", "c"), Optional.of("test")));
        assertThat(statement("COMMENT ON COLUMN a.b.c.d IS 'test'")).isEqualTo(new Comment(Comment.Type.COLUMN, QualifiedName.of("a", "b", "c", "d"), Optional.of("test")));
    }

    @Test
    public void testRenameColumn() {
        assertThat(statement("ALTER TABLE foo.t RENAME COLUMN a TO b")).isEqualTo(new RenameColumn(QualifiedName.of("foo", "t"), identifier("a"), identifier("b"), false, false));
        assertThat(statement("ALTER TABLE IF EXISTS foo.t RENAME COLUMN a TO b")).isEqualTo(new RenameColumn(QualifiedName.of("foo", "t"), identifier("a"), identifier("b"), true, false));
        assertThat(statement("ALTER TABLE foo.t RENAME COLUMN IF EXISTS a TO b")).isEqualTo(new RenameColumn(QualifiedName.of("foo", "t"), identifier("a"), identifier("b"), false, true));
        assertThat(statement("ALTER TABLE IF EXISTS foo.t RENAME COLUMN IF EXISTS a TO b")).isEqualTo(new RenameColumn(QualifiedName.of("foo", "t"), identifier("a"), identifier("b"), true, true));
    }

    @Test
    public void testRenameView() {
        assertThat(statement("ALTER VIEW a RENAME TO b")).isEqualTo(new RenameView(QualifiedName.of("a"), QualifiedName.of("b")));
    }

    @Test
    public void testAlterViewSetAuthorization() {
        assertThat(statement("ALTER VIEW foo.bar.baz SET AUTHORIZATION qux")).isEqualTo(new SetViewAuthorization(QualifiedName.of("foo", "bar", "baz"), new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("qux"))));

        assertThat(statement("ALTER VIEW foo.bar.baz SET AUTHORIZATION USER qux")).isEqualTo(new SetViewAuthorization(QualifiedName.of("foo", "bar", "baz"), new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("qux"))));

        assertThat(statement("ALTER VIEW foo.bar.baz SET AUTHORIZATION ROLE qux")).isEqualTo(new SetViewAuthorization(QualifiedName.of("foo", "bar", "baz"), new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("qux"))));
    }

    @Test
    public void testAnalyze() {
        final QualifiedName tableName1 = qualifiedName(location(1, 9), "foo");
        final QualifiedName tableName2 = qualifiedName(location(1, 17), "foo");
        final QualifiedName tableName3 = qualifiedName(location(1, 25), "foo");

        assertThat(statement("ANALYZE foo")).isEqualTo(new Analyze(location(1, 1), tableName1, ImmutableList.of()));

        assertThat(statement("ANALYZE foo WITH ( \"string\" = 'bar', \"long\" = 42, computed = concat('ban', 'ana'), a = ARRAY[ 'v1', 'v2' ] )")).isEqualTo(
                new Analyze(location(1, 1), tableName1, ImmutableList.of(
                        new Property(location(1, 20),
                                new Identifier(location(1, 20), "string", true),
                                new StringLiteral(location(1, 31), "bar")),
                        new Property(location(1, 38),
                                new Identifier(location(1, 38), "long", true),
                                new LongLiteral(location(1, 47), "42")),
                        new Property(location(1, 51),
                                new Identifier(location(1, 51), "computed", false),
                                new FunctionCall(location(1, 62), qualifiedName(location(1, 62), "concat"), ImmutableList.of(
                                        new StringLiteral(location(1, 69), "ban"),
                                        new StringLiteral(location(1, 76), "ana")))),
                        new Property(location(1, 84),
                                new Identifier(location(1, 84), "a", false),
                                new ArrayConstructor(location(1, 88), ImmutableList.of(
                                        new StringLiteral(location(1, 95), "v1"),
                                        new StringLiteral(location(1, 101), "v2")))))));

        assertThat(statement("EXPLAIN ANALYZE foo")).isEqualTo(
                new Explain(location(1, 1), false, false,
                        new Analyze(location(1, 9), tableName2, ImmutableList.of()), ImmutableList.of()));

        assertThat(statement("EXPLAIN ANALYZE ANALYZE foo")).isEqualTo(
                new Explain(location(1, 1), true, false,
                        new Analyze(location(1, 17), tableName3, ImmutableList.of()), ImmutableList.of()));
    }

    @Test
    public void testAddColumn()
    {
        assertThat(statement("ALTER TABLE foo.t ADD COLUMN c bigint"))
                .ignoringLocation()
                .isEqualTo(new AddColumn(
                        QualifiedName.of("foo", "t"),
                        new ColumnDefinition(identifier("c"), simpleType(location(1, 31), "bigint"), true, emptyList(), Optional.empty()), false, false));

        assertThat(statement("ALTER TABLE foo.t ADD COLUMN d double NOT NULL"))
                .ignoringLocation()
                .isEqualTo(new AddColumn(
                        QualifiedName.of("foo", "t"),
                        new ColumnDefinition(identifier("d"), simpleType(location(1, 31), "double"), false, emptyList(), Optional.empty()), false, false));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t ADD COLUMN d double NOT NULL"))
                .ignoringLocation()
                .isEqualTo(new AddColumn(
                        QualifiedName.of("foo", "t"),
                        new ColumnDefinition(identifier("d"), simpleType(location(1, 31), "double"), false, emptyList(), Optional.empty()), true, false));

        assertThat(statement("ALTER TABLE foo.t ADD COLUMN IF NOT EXISTS d double NOT NULL"))
                .ignoringLocation()
                .isEqualTo(new AddColumn(
                        QualifiedName.of("foo", "t"),
                        new ColumnDefinition(identifier("d"), simpleType(location(1, 31), "double"), false, emptyList(), Optional.empty()), false, true));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t ADD COLUMN IF NOT EXISTS d double NOT NULL"))
                .ignoringLocation()
                .isEqualTo(new AddColumn(
                        QualifiedName.of("foo", "t"),
                        new ColumnDefinition(identifier("d"), simpleType(location(1, 31), "double"), false, emptyList(), Optional.empty()), true, true));
    }

    @Test
    public void testDropColumn()
    {
        assertThat(statement("ALTER TABLE foo.t DROP COLUMN c")).isEqualTo(
                new DropColumn(location(1, 1),
                        QualifiedName.of(ImmutableList.of(
                                new Identifier(location(1, 13), "foo", false),
                                new Identifier(location(1, 17), "t", false))),
                        new Identifier(location(1, 31), "c", false),
                        false, false));

        assertThat(statement("ALTER TABLE \"t x\" DROP COLUMN \"c d\"")).isEqualTo(
                new DropColumn(location(1, 1),
                        QualifiedName.of(ImmutableList.of(
                                new Identifier(location(1, 13), "t x", true))),
                        new Identifier(location(1, 31), "c d", true),
                        false, false));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t DROP COLUMN c")).isEqualTo(
                new DropColumn(location(1, 1), QualifiedName.of(ImmutableList.of(
                        new Identifier(location(1, 23), "foo", false),
                        new Identifier(location(1, 27), "t", false))),
                        new Identifier(location(1, 41), "c", false),
                        true, false));

        assertThat(statement("ALTER TABLE foo.t DROP COLUMN IF EXISTS c")).isEqualTo(
                new DropColumn(location(1, 1), QualifiedName.of(ImmutableList.of(
                        new Identifier(location(1, 13), "foo", false),
                        new Identifier(location(1, 17), "t", false))),
                        new Identifier(location(1, 41), "c", false), false, true));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t DROP COLUMN IF EXISTS c")).isEqualTo(
                new DropColumn(location(1, 1), QualifiedName.of(ImmutableList.of(
                        new Identifier(location(1, 23), "foo", false),
                        new Identifier(location(1, 27), "t", false))),
                        new Identifier(location(1, 51), "c", false), true, true));
    }

    @Test
    public void testAlterTableSetAuthorization()
    {
        assertThat(statement("ALTER TABLE foo.bar.baz SET AUTHORIZATION qux")).isEqualTo(
                new SetTableAuthorization(location(1, 1),
                        QualifiedName.of(ImmutableList.of(
                                new Identifier(location(1, 13), "foo", false),
                                new Identifier(location(1, 17), "bar", false),
                                new Identifier(location(1, 21), "baz", false))),
                        new PrincipalSpecification(
                                PrincipalSpecification.Type.UNSPECIFIED,
                                new Identifier(location(1, 43), "qux", false))));

        assertThat(statement("ALTER TABLE foo.bar.baz SET AUTHORIZATION USER qux")).isEqualTo(
                new SetTableAuthorization(location(1, 1),
                        QualifiedName.of(ImmutableList.of(
                                new Identifier(location(1, 13), "foo", false),
                                new Identifier(location(1, 17), "bar", false),
                                new Identifier(location(1, 21), "baz", false))),
                        new PrincipalSpecification(
                                PrincipalSpecification.Type.USER,
                                new Identifier(location(1, 48), "qux", false))));

        assertThat(statement("ALTER TABLE foo.bar.baz SET AUTHORIZATION ROLE qux")).isEqualTo(
                new SetTableAuthorization(location(1, 1),
                        QualifiedName.of(ImmutableList.of(
                                new Identifier(location(1, 13), "foo", false),
                                new Identifier(location(1, 17), "bar", false),
                                new Identifier(location(1, 21), "baz", false))),
                        new PrincipalSpecification(PrincipalSpecification.Type.ROLE,
                                new Identifier(location(1, 48), "qux", false))));
    }

    private Query newCreateViewQuery(int queryColumn, int columnCount, int tableNameLocation)
    {
        return simpleQuery(location(1, queryColumn),
                selectList(location(1, queryColumn), new AllColumns(location(1, columnCount), Optional.empty(), ImmutableList.of())),
                new Table(location(1, tableNameLocation), qualifiedName(location(1, tableNameLocation), "t")));
    }

    @Test
    public void testCreateView()
    {
        final Query createViewQuery1 = this.newCreateViewQuery(18, 25, 32);
        final Query createViewQuery2 = this.newCreateViewQuery(29, 36, 43);
        final Query createViewQuery3 = this.newCreateViewQuery(35, 42, 49);
        final Query createViewQuery4 = this.newCreateViewQuery(53, 60, 67);
        final Query createViewQuery5 = this.newCreateViewQuery(46, 53, 60);
        final Query createViewQuery6 = this.newCreateViewQuery(36, 43, 50);
        final Query createViewQuery7 = this.newCreateViewQuery(24, 31, 38);
        final Query createViewQuery8 = this.newCreateViewQuery(31, 38, 45);
        final Query createViewQuery9 = this.newCreateViewQuery(48, 55, 62);

        assertThat(statement("CREATE VIEW a AS SELECT * FROM t")).isEqualTo(
                new CreateView(location(1, 1), qualifiedName(location(1, 13), "a"),
                        createViewQuery1, false, Optional.empty(), Optional.empty()));

        assertThat(statement("CREATE OR REPLACE VIEW a AS SELECT * FROM t")).isEqualTo(
                new CreateView(location(1, 1), qualifiedName(location(1, 24), "a"),
                        createViewQuery2, true, Optional.empty(), Optional.empty()));

        assertThat(statement("CREATE VIEW a SECURITY DEFINER AS SELECT * FROM t")).isEqualTo(
                new CreateView(location(1, 1), qualifiedName(location(1, 13), "a"),
                        createViewQuery3, false, Optional.empty(), Optional.of(CreateView.Security.DEFINER)));

        assertThat(statement("CREATE VIEW a SECURITY INVOKER AS SELECT * FROM t")).isEqualTo(
                new CreateView(location(1, 1), qualifiedName(location(1, 13), "a"),
                        createViewQuery3, false, Optional.empty(), Optional.of(CreateView.Security.INVOKER)));

        assertThat(statement("CREATE VIEW a COMMENT 'comment' SECURITY DEFINER AS SELECT * FROM t")).isEqualTo(
                new CreateView(location(1, 1),  qualifiedName(location(1, 13), "a"),
                        createViewQuery4, false, Optional.of("comment"), Optional.of(CreateView.Security.DEFINER)));

        assertThat(statement("CREATE VIEW a COMMENT '' SECURITY INVOKER AS SELECT * FROM t")).isEqualTo(
                new CreateView(location(1, 1),  qualifiedName(location(1, 13), "a"),
                        createViewQuery5, false, Optional.of(""), Optional.of(CreateView.Security.INVOKER)));

        assertThat(statement("CREATE VIEW a COMMENT 'comment' AS SELECT * FROM t")).isEqualTo(
                new CreateView(location(1, 1), qualifiedName(location(1, 13), "a"),
                        createViewQuery6, false, Optional.of("comment"), Optional.empty()));

        assertThat(statement("CREATE VIEW a COMMENT '' AS SELECT * FROM t")).isEqualTo(
                new CreateView(location(1, 1), qualifiedName(location(1, 13), "a"),
                        createViewQuery2, false, Optional.of(""), Optional.empty()));

        assertThat(statement("CREATE VIEW bar.foo AS SELECT * FROM t")).isEqualTo(
                new CreateView(location(1, 1), QualifiedName.of(ImmutableList.of(
                        new Identifier(location(1, 13), "bar", false),
                        new Identifier(location(1, 17), "foo", false))),
                        createViewQuery7, false, Optional.empty(), Optional.empty()));

        assertThat(statement("CREATE VIEW \"awesome view\" AS SELECT * FROM t")).isEqualTo(
                new CreateView(location(1, 1), QualifiedName.of(ImmutableList.of(
                        new Identifier(location(1,13),"awesome view", true))),
                        createViewQuery8, false, Optional.empty(), Optional.empty()));

        assertThat(statement("CREATE VIEW \"awesome schema\".\"awesome view\" AS SELECT * FROM t")).isEqualTo(
                new CreateView(location(1, 1), QualifiedName.of(ImmutableList.of(
                        new Identifier(location(1, 13), "awesome schema", true),
                        new Identifier(location(1, 30), "awesome view", true))),
                        createViewQuery9, false, Optional.empty(), Optional.empty()));
    }

    @Test
    public void testGrant() {
//        assertThat(statement("GRANT INSERT, DELETE ON t TO u")).isEqualTo(
//                new Grant(location(1, 1),
//                        Optional.of(ImmutableList.of("INSERT", "DELETE")),
//                        Optional.empty(),
//                        qualifiedName(location(1, 25), "t"),
//                        new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier(location(1, 30), "u", false)),
//                        false));
//
//        assertThat(statement("GRANT SELECT ON t TO ROLE PUBLIC WITH GRANT OPTION")).isEqualTo(
//                new Grant(location(1, 1),
//                        Optional.of(ImmutableList.of("SELECT")),
//                        Optional.empty(),
//                        qualifiedName(location(1, 17), "t"),
//                        new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier(location(1, 27), "PUBLIC", false)),
//                        true));
//
//        assertThat(statement("GRANT ALL PRIVILEGES ON TABLE t TO USER u")).isEqualTo(
//                new Grant(location(1, 1),
//                        Optional.empty(),
//                        Optional.of(GrantOnType.TABLE),
//                        qualifiedName(location(1, 31), "t"),
//                        new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier(location(1, 41), "u", false)),
//                        false));

//        assertThat(statement("GRANT DELETE ON \"t\" TO ROLE \"public\" WITH GRANT OPTION")).isEqualTo(
//                new Grant(location(1, 1),
//                        Optional.of(ImmutableList.of("DELETE")),
//                        Optional.empty(),
//                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 18), "t", false))),
//                        new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier(location(1, 30), "public", false)),
//                        true));

//        Optional<List<String>> privileges = Optional.of(ImmutableList.of("DELETE"));
//        QualifiedName name = qualifiedName(location(1, 18), "t");

//        Identifier principleName = new Identifier("public", false);
//
//        Grant grant = new Grant(
//                privileges,
//                Optional.empty(),
//                name,
//                new PrincipalSpecification(PrincipalSpecification.Type.ROLE, principleName),
//                true);
//
//        Statement grant2 = new SqlParser().createStatement("GRANT DELETE ON \"t\" TO ROLE \"public\" WITH GRANT OPTION", new ParsingOptions(AS_DECIMAL));
//
//        assertThat(statement("GRANT DELETE ON \"t\" TO ROLE \"public\" WITH GRANT OPTION")).isEqualTo(grant2);


        assertThat(statement("GRANT DELETE ON \"t\" TO ROLE \"public\" WITH GRANT OPTION")).isEqualTo(
                new Grant(
                        Optional.of(ImmutableList.of("DELETE")),
                        Optional.empty(),
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("public")),
                        true));

//        assertThat(statement("GRANT SELECT ON SCHEMA s TO USER u")).isEqualTo(
//                new Grant(location(1, 1),
//                        Optional.of(ImmutableList.of("SELECT")),
//                        Optional.of(GrantOnType.SCHEMA),
//                        qualifiedName(location(1, 24), "s"),
//                        new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier(location(1, 34), "u", false)),
//                        true));
    }

    @Test
    public void testRevoke() {
        assertThat(statement("REVOKE INSERT, DELETE ON t FROM u")).isEqualTo(
                new Revoke(location(1, 1),
                false,
                Optional.of(ImmutableList.of("INSERT", "DELETE")),
                Optional.empty(),
                qualifiedName(location(1, 26), "t"),
                new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier(location(1, 33), "u", false))));

        assertThat(statement("REVOKE GRANT OPTION FOR SELECT ON t FROM ROLE PUBLIC")).isEqualTo(
                new Revoke(location(1, 1),
                true,
                Optional.of(ImmutableList.of("SELECT")),
                Optional.empty(),
                qualifiedName(location(1, 35), "t"),
                new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier(location(1, 47), "PUBLIC", false))));

        assertThat(statement("REVOKE ALL PRIVILEGES ON TABLE t FROM USER u")).isEqualTo(
                new Revoke(location(1, 1),
                false,
                Optional.empty(),
                Optional.of(GrantOnType.TABLE),
                qualifiedName(location(1, 32), "t"),
                new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier(location(1, 44), "u", false))));

        assertThat(statement("REVOKE DELETE ON TABLE \"t\" FROM \"u\"")).isEqualTo(
                new Revoke(location(1, 1),
                false,
                Optional.of(ImmutableList.of("DELETE")),
                Optional.of(GrantOnType.TABLE),
                qualifiedName(location(1, 24), "t"),
                new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier(location(1, 35), "u", false))));

        assertThat(statement("REVOKE SELECT ON SCHEMA s FROM USER u")).isEqualTo(new Revoke(
                false,
                Optional.of(ImmutableList.of("SELECT")),
                Optional.of(GrantOnType.SCHEMA),
                qualifiedName(location(1, 25), "s"),
                new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier(location(1, 37), "u", false))));
    }

    @Test
    public void testShowGrants() {
        assertThat(statement("SHOW GRANTS ON TABLE t")).isEqualTo(
                new ShowGrants(location(1, 1), true,
                        Optional.of(qualifiedName(location(1, 22), "t"))));

        assertThat(statement("SHOW GRANTS ON t")).isEqualTo(
                new ShowGrants(location(1, 1), false,
                        Optional.of(qualifiedName(location(1, 16), "t"))));

        assertThat(statement("SHOW GRANTS")).isEqualTo(
                new ShowGrants(location(1, 1), false,
                        Optional.empty()));
    }

    @Test
    public void testShowRoles() {
        assertThat(statement("SHOW ROLES")).isEqualTo(
                new ShowRoles(location(1, 1),
                        Optional.empty(), false));

        assertThat(statement("SHOW ROLES FROM foo")).isEqualTo(
                new ShowRoles(location(1, 1),
                        Optional.of(new Identifier(location(1, 17), "foo", false)), false));

        assertThat(statement("SHOW ROLES IN foo")).isEqualTo(
                new ShowRoles(location(1, 1),
                        Optional.of(new Identifier(location(1, 15), "foo", false)), false));

        assertThat(statement("SHOW CURRENT ROLES")).isEqualTo(
                new ShowRoles(location(1, 1), Optional.empty(), true));

        assertThat(statement("SHOW CURRENT ROLES FROM foo")).isEqualTo(
                new ShowRoles(location(1, 1),
                        Optional.of(new Identifier(location(1, 25), "foo", false)), true));

        assertThat(statement("SHOW CURRENT ROLES IN foo")).isEqualTo(
                new ShowRoles(location(1, 1),
                        Optional.of(new Identifier(location(1, 23), "foo", false)), true));
    }

    @Test
    public void testShowRoleGrants() {
        assertThat(statement("SHOW ROLE GRANTS")).isEqualTo(new ShowRoleGrants(Optional.empty(), Optional.empty()));
        assertThat(statement("SHOW ROLE GRANTS FROM catalog")).isEqualTo(new ShowRoleGrants(Optional.of(new Identifier("catalog"))));
    }

    @Test
    public void testSetPath() {
        assertThat(statement("SET PATH iLikeToEat.apples, andBananas")).isEqualTo(
                new SetPath(location(1, 1),
                        new PathSpecification(location(1, 10), ImmutableList.of(
                        new PathElement(location(1, 10),
                                new Identifier(location(1, 10), "iLikeToEat", false),
                                new Identifier(location(1, 21), "apples", false)),
                                new PathElement(location(1, 29),
                                        new Identifier(location(1, 29), "andBananas", false))))));

        assertThat(statement("SET PATH \"schemas,with\".\"grammar.in\", \"their!names\"")).isEqualTo(
                new SetPath(location(1, 1),
                        new PathSpecification(location(1, 10), ImmutableList.of(
                                new PathElement(location(1, 10),
                                        new Identifier(location(1, 10), "schemas,with", true),
                                        new Identifier(location(1, 25), "grammar.in", true)),
                                new PathElement(location(1, 39),
                                        new Identifier(location(1, 39), "their!names", true))))));

        assertThatThrownBy(() -> {
            assertThat(statement("SET PATH one.too.many, qualifiers")).isEqualTo(
                    new SetPath(
                            new PathSpecification(Optional.empty(), ImmutableList.of(
                            new PathElement(Optional.empty(), new Identifier("dummyValue"))))));
        })
                .isInstanceOf(ParsingException.class)
                .hasMessage("line 1:17: mismatched input '.'. Expecting: ',', <EOF>");

        assertThatThrownBy(() -> SQL_PARSER.createStatement("SET PATH ", new ParsingOptions()))
                .isInstanceOf(ParsingException.class)
                .hasMessage("line 1:10: mismatched input '<EOF>'. Expecting: <identifier>");
    }

    @Test
    public void testWith() {
        assertThat(statement("WITH a (t, u) AS (SELECT * FROM x), b AS (SELECT * FROM y) TABLE z")).isEqualTo(
                new Query(location(1, 1),
                        Optional.of(new With(location(1, 1), false, ImmutableList.of(
                                new WithQuery(location(1, 6),
                                        new Identifier(location(1, 6), "a", false),
                                        simpleQuery(location(1, 19),
                                                selectList(location(1, 19),
                                                        new AllColumns(location(1, 26), Optional.empty(), ImmutableList.of())),
                                                new Table(location(1, 33), qualifiedName(location(1, 33), "x"))),
                                        Optional.of(ImmutableList.of(
                                                new Identifier(location(1, 9), "t", false),
                                                new Identifier(location(1, 12), "u", false)))),
                                new WithQuery(location(1, 37),
                                        new Identifier(location(1, 37), "b", false),
                                        simpleQuery(location(1, 43),
                                                selectList(location(1, 43),
                                                        new AllColumns(location(1, 50), Optional.empty(), ImmutableList.of())),
                                                new Table(location(1, 57), qualifiedName(location(1, 57), "y"))),
                                        Optional.empty())))),
                        new Table(location(1, 60), qualifiedName(location(1, 66), "z")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertThat(statement("WITH RECURSIVE a AS (SELECT * FROM x) TABLE y")).isEqualTo(
                new Query(location(1, 1),
                        Optional.of(new With(location(1, 1), true, ImmutableList.of(
                                new WithQuery(location(1, 16), new Identifier(location(1, 16), "a", false),
                                        simpleQuery(location(1, 22),
                                                selectList(location(1, 22),
                                                        new AllColumns(location(1, 29),
                                                                Optional.empty(), ImmutableList.of())),
                                                new Table(location(1, 36), qualifiedName(location(1, 36), "x"))),
                                        Optional.empty())))),
                        new Table(location(1, 39), qualifiedName(location(1, 45), "y")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testImplicitJoin() {
        assertThat(statement("SELECT * FROM a, b")).isEqualTo(
                simpleQuery(location(1, 1),
                        selectList(location(1, 1),
                                new AllColumns(location(1, 8), Optional.empty(), ImmutableList.of())),
                        new Join(location(1, 1), Join.Type.IMPLICIT,
                                new Table(location(1, 15), qualifiedName(location(1, 15), "a")),
                                new Table(location(1, 18), qualifiedName(location(1, 18), "b")),
                                Optional.empty())));
    }

    @Test
    public void testExplain() {
        assertThat(statement("EXPLAIN SELECT * FROM t")).isEqualTo(
                new Explain(location(1, 1),
                        false, false,
                        simpleQuery(location(1, 9), selectList(location(1, 9),
                                new AllColumns(location(1, 16), Optional.empty(), ImmutableList.of())),
                                new Table(location(1, 23), qualifiedName(location(1, 23), "t"))),
                        ImmutableList.of()));

        assertThat(statement("EXPLAIN (TYPE LOGICAL) SELECT * FROM t")).isEqualTo(
                new Explain(location(1, 1),
                        false, false,
                        simpleQuery(location(1, 24), selectList(location(1, 24),
                                new AllColumns(location(1, 31), Optional.empty(), ImmutableList.of())),
                                new Table(location(1, 38), qualifiedName(location(1, 38), "t"))),
                        ImmutableList.of(new ExplainType(location(1, 10), ExplainType.Type.LOGICAL))));

        assertThat(statement("EXPLAIN (TYPE LOGICAL, FORMAT TEXT) SELECT * FROM t")).isEqualTo(
                new Explain(location(1, 1),
                        false, false,
                simpleQuery(location(1, 37), selectList(location(1, 37),
                        new AllColumns(location(1, 44), Optional.empty(), ImmutableList.of())),
                        new Table(location(1, 51), qualifiedName(location(1, 51), "t"))),
                ImmutableList.of(
                        new ExplainType(location(1, 10), ExplainType.Type.LOGICAL),
                        new ExplainFormat(location(1, 24), ExplainFormat.Type.TEXT))));
    }

    @Test
    public void testExplainVerbose() {
        assertThat(statement("EXPLAIN VERBOSE SELECT * FROM t")).isEqualTo(
                new Explain(location(1, 1),
                        false, true,
                        simpleQuery(location(1, 1), selectList(location(1, 1),
                                new AllColumns(location(1, 24), Optional.empty(), ImmutableList.of())),
                                new Table(location(1, 31), qualifiedName(location(1, 31), "t"))), ImmutableList.of()));
    }

    // issue with verbose not being detected correctly...???
    @Test
    public void testExplainVerboseTypeLogical() {
        Statement stmt = new SqlParser().createStatement("EXPLAIN VERBOSE (type LOGICAL) SELECT * FROM t", new ParsingOptions(AS_DECIMAL));

        Statement explainStatement = new Explain(location(1, 1),
                false, true,
                simpleQuery(selectList(location(1, 32),
                        new AllColumns(location(1, 24), Optional.empty(), ImmutableList.of())),
                        new Table(location(1, 46), qualifiedName(location(1, 46), "t"))),
                ImmutableList.of(
                        new ExplainType(location(1, 23), ExplainType.Type.LOGICAL)));

        assertThat(statement("EXPLAIN VERBOSE (type LOGICAL) SELECT * FROM t")).isEqualTo(explainStatement);
    }

    @Test
    public void testExplainAnalyze() {
        assertThat(statement("EXPLAIN ANALYZE SELECT * FROM t")).isEqualTo(
                new Explain(location(1, 1),
                        true, false,
                        simpleQuery(location(1, 17), selectList(location(1, 17),
                                new AllColumns(location(1, 24), Optional.empty(), ImmutableList.of())),
                                new Table(location(1, 31), qualifiedName(location(1, 31), "t"))),
                        ImmutableList.of()));
    }

    @Test
    public void testExplainAnalyzeTypeDistributed() {
        assertThat(statement("EXPLAIN ANALYZE (type DISTRIBUTED) SELECT * FROM t")).isEqualTo(
                new Explain(location(1, 1),
                        true, false,
                        simpleQuery(location(1, 36), selectList(location(1, 36),
                                new AllColumns(location(1, 43), Optional.empty(), ImmutableList.of())),
                                new Table(location(1, 50), qualifiedName(location(1, 50), "t"))),
                        ImmutableList.of(new ExplainType(location(1, 18), ExplainType.Type.DISTRIBUTED))));
    }

    @Test
    public void testExplainAnalyzeVerbose() {
        assertThat(statement("EXPLAIN ANALYZE VERBOSE SELECT * FROM t")).isEqualTo(new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), true, true, ImmutableList.of()));
    }

    @Test
    public void testExplainAnalyzeVerboseTypeDistributed() {
        assertThat(statement("EXPLAIN ANALYZE VERBOSE (type DISTRIBUTED) SELECT * FROM t")).isEqualTo(new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), true, true, ImmutableList.of(new ExplainType(ExplainType.Type.DISTRIBUTED))));
    }

    @Test
    public void testJoinPrecedence() {
        assertThat(statement("SELECT * FROM a CROSS JOIN b LEFT JOIN c ON true")).isEqualTo(simpleQuery(location(1, 1),
                selectList(location(1, 1), new AllColumns(location(1, 1), Optional.empty(), ImmutableList.of())),
                new Join(location(1, 15),
                        Join.Type.LEFT,
                        new Join(location(1, 15),
                                Join.Type.CROSS,
                                new Table(location(1, 15), qualifiedName(location(1, 15), "a")),
                                new Table(location(1, 28), qualifiedName(location(1, 28),"b")),
                                Optional.empty()),
                        new Table(location(1, 40), qualifiedName(location(1, 40), "c")),
                        Optional.of(new JoinOn(BooleanLiteral.TRUE_LITERAL)))));

        assertThat(statement("SELECT * FROM a CROSS JOIN b NATURAL JOIN c CROSS JOIN d NATURAL JOIN e")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Join(
                        Join.Type.INNER,
                        new Join(
                                Join.Type.CROSS,
                                new Join(
                                        Join.Type.INNER,
                                        new Join(
                                                Join.Type.CROSS,
                                                new Table(QualifiedName.of("a")),
                                                new Table(QualifiedName.of("b")),
                                                Optional.empty()),
                                        new Table(QualifiedName.of("c")),
                                        Optional.of(new NaturalJoin())),
                                new Table(QualifiedName.of("d")),
                                Optional.empty()),
                        new Table(QualifiedName.of("e")),
                        Optional.of(new NaturalJoin()))));
    }

    @Test
    public void testUnnest() {
        assertThat(statement("SELECT * FROM t CROSS JOIN UNNEST(a)")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Join(
                        Join.Type.CROSS,
                        new Table(QualifiedName.of("t")),
                        new Unnest(ImmutableList.of(new Identifier("a")), false),
                        Optional.empty())));
        assertThat(statement("SELECT * FROM t CROSS JOIN UNNEST(a, b) WITH ORDINALITY")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Join(
                        Join.Type.CROSS,
                        new Table(QualifiedName.of("t")),
                        new Unnest(ImmutableList.of(new Identifier("a"), new Identifier("b")), true),
                        Optional.empty())));
        assertThat(statement("SELECT * FROM t FULL JOIN UNNEST(a) AS tmp (c) ON true")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Join(
                        Join.Type.FULL,
                        new Table(QualifiedName.of("t")),
                        new AliasedRelation(new Unnest(ImmutableList.of(new Identifier("a")), false), new Identifier("tmp"), ImmutableList.of(new Identifier("c"))),
                        Optional.of(new JoinOn(BooleanLiteral.TRUE_LITERAL)))));
    }

    @Test
    public void testLateral()
    {
        Lateral lateralRelation = new Lateral(
                query(new Values(ImmutableList.of(new LongLiteral("1")))));

        assertThat(statement("SELECT * FROM t, LATERAL (VALUES 1) a(x)")).isEqualTo(
                simpleQuery(location(1, 1),
                        selectList(location(1, 1), new AllColumns()),
                        new Join(location(1, 1),
                                Join.Type.IMPLICIT,
                                new Table(location(1, 15), QualifiedName.of(
                                        ImmutableList.of(new Identifier(location(1, 15), "t", false)))),
                                new AliasedRelation(location(1, 18),
                                        lateralRelation,
                                        new Identifier(location(1, 37), "a", false),
                                        ImmutableList.of(new Identifier(location(1, 39), "x", false))),
                                Optional.empty())));

//        assertStatement("SELECT * FROM t CROSS JOIN LATERAL (VALUES 1) ",
//                simpleQuery(
//                        selectList(new AllColumns()),
//                        new Join(
//                                Join.Type.CROSS,
//                                new Table(QualifiedName.of("t")),
//                                lateralRelation,
//                                Optional.empty())));
//
//        assertStatement("SELECT * FROM t FULL JOIN LATERAL (VALUES 1) ON true",
//                simpleQuery(
//                        selectList(new AllColumns()),
//                        new Join(
//                                Join.Type.FULL,
//                                new Table(QualifiedName.of("t")),
//                                lateralRelation,
//                                Optional.of(new JoinOn(BooleanLiteral.TRUE_LITERAL)))));
    }

    @Test
    public void testStartTransaction()
    {
        assertThat(statement("START TRANSACTION")).isEqualTo(
                new StartTransaction(ImmutableList.of()));

        assertThat(statement("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")).isEqualTo(
                new StartTransaction(ImmutableList.of(
                        new Isolation(location(1, 35), Isolation.Level.READ_UNCOMMITTED))));

        assertThat(statement("START TRANSACTION ISOLATION LEVEL READ COMMITTED")).isEqualTo(
                new StartTransaction(ImmutableList.of(
                        new Isolation(location(1, 35), Isolation.Level.READ_COMMITTED))));

        assertThat(statement("START TRANSACTION ISOLATION LEVEL REPEATABLE READ")).isEqualTo(
                new StartTransaction(ImmutableList.of(
                        new Isolation(location(1, 35), Isolation.Level.REPEATABLE_READ))));

        assertThat(statement("START TRANSACTION ISOLATION LEVEL SERIALIZABLE")).isEqualTo(
                new StartTransaction(ImmutableList.of(
                        new Isolation(location(1, 35), Isolation.Level.SERIALIZABLE))));

        assertThat(statement("START TRANSACTION READ ONLY")).isEqualTo(
                new StartTransaction(ImmutableList.of(
                        new TransactionAccessMode(location(1, 19), true))));

        assertThat(statement("START TRANSACTION READ WRITE")).isEqualTo(
                new StartTransaction(ImmutableList.of(
                        new TransactionAccessMode(location(1, 19), false))));

        assertThat(statement("START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY")).isEqualTo(
                new StartTransaction(ImmutableList.of(
                        new Isolation(location(1, 35), Isolation.Level.READ_COMMITTED),
                        new TransactionAccessMode(location(1, 51), true))));

        assertThat(statement("START TRANSACTION READ ONLY, ISOLATION LEVEL READ COMMITTED")).isEqualTo(
                new StartTransaction(ImmutableList.of(
                        new TransactionAccessMode(location(1, 19), true),
                        new Isolation(location(1, 46), Isolation.Level.READ_COMMITTED))));

        assertThat(statement("START TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE")).isEqualTo(
                new StartTransaction(ImmutableList.of(
                        new TransactionAccessMode(location(1, 19), false),
                        new Isolation(location(1, 47), Isolation.Level.SERIALIZABLE))));
    }

    @Test
    public void testCommit()
    {
        assertThat(statement("COMMIT")).isEqualTo(new Commit(location(1, 1)));
        assertThat(statement("COMMIT WORK")).isEqualTo(new Commit(location(1, 1)));
    }

    @Test
    public void testRollback()
    {
        assertThat(statement("ROLLBACK")).isEqualTo(new Rollback(location(1, 1)));
        assertThat(statement("ROLLBACK WORK")).isEqualTo(new Rollback(location(1, 1)));
    }

    @Test
    public void testAtTimeZone()
    {
        final TimestampLiteral timestamp = new TimestampLiteral(location(1, 8),"2012-10-31 01:00 UTC");
        final StringLiteral location = new StringLiteral(location(1, 54), "America/Los_Angeles");
        final AtTimeZone timezone = new AtTimeZone(location(1, 41), timestamp, location);

        assertThat(statement("SELECT timestamp '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles'")).isEqualTo(
                simpleQuery(location(1, 1),
                        new Select(location(1, 1), false, ImmutableList.of(
                                new SingleColumn(location(1, 8), timezone, Optional.empty())))));
    }

    @Test
    public void testLambda()
    {
        assertThat(expression("() -> x")).isEqualTo(
                new LambdaExpression(location(1, 1),
                        ImmutableList.of(),
                        new Identifier(location(1, 7),"x", false)));

        assertThat(expression("x -> sin(x)")).isEqualTo(
                new LambdaExpression(location(1, 1),
                        ImmutableList.of(new LambdaArgumentDeclaration(new Identifier(location(1, 1), "x", false))),
                        new FunctionCall(location(1, 6),
                                qualifiedName(location(1, 6), "sin"),
                                ImmutableList.of(new Identifier(location(1, 10), "x", false)))));

        assertThat(expression("(x, y) -> mod(x, y)")).isEqualTo(
                new LambdaExpression(location(1, 1),
                        ImmutableList.of(
                                new LambdaArgumentDeclaration(new Identifier(location(1, 2), "x", false)),
                                new LambdaArgumentDeclaration(new Identifier(location(1, 5), "y", false))),
                        new FunctionCall(location(1, 11), qualifiedName(location(1, 11), "mod"),
                                ImmutableList.of(
                                        new Identifier(location(1, 15), "x", false),
                                        new Identifier(location(1, 18), "y", false)))));
    }

    @Test
    public void testNonReserved()
    {
        assertThat(statement("SELECT zone FROM t")).isEqualTo(
                simpleQuery(location(1, 1),
                        selectList(location(1, 1),
                                new Identifier(location(1, 8), "zone", false)),
                        new Table(location(1, 18),
                                qualifiedName(location(1, 18), "t"))));

        assertThat(statement("SELECT INCLUDING, EXCLUDING, PROPERTIES FROM t")).isEqualTo(
                simpleQuery(location(1, 1),
                        selectList(location(1, 1),
                                new Identifier(location(1, 8), "INCLUDING", false),
                                new Identifier(location(1, 19), "EXCLUDING", false),
                                new Identifier(location(1, 30), "PROPERTIES", false)),
                        new Table(location(1, 46),
                                qualifiedName(location(1, 46), "t"))));

        assertThat(statement("SELECT ALL, SOME, ANY FROM t")).isEqualTo(
                simpleQuery(location(1, 1),
                        selectList(location(1, 1),
                                new Identifier(location(1, 8), "ALL", false),
                                new Identifier(location(1, 13), "SOME", false),
                                new Identifier(location(1, 19), "ANY", false)),
                        new Table(location(1, 28),
                                qualifiedName(location(1, 28), "t"))));

        assertThat(expression("stats")).isEqualTo(new Identifier(location(1, 1), "stats", false));
        assertThat(expression("nfd")).isEqualTo(new Identifier(location(1, 1), "nfd", false));
        assertThat(expression("nfc")).isEqualTo(new Identifier(location(1, 1), "nfc", false));
        assertThat(expression("nfkd")).isEqualTo(new Identifier(location(1, 1), "nfkd", false));
        assertThat(expression("nfkc")).isEqualTo(new Identifier(location(1, 1), "nfkc", false));
    }

    @Test
    public void testBinaryLiteralToHex()
    {
        // note that toHexString() always outputs in upper case
        assertEquals(new BinaryLiteral("ab 01").toHexString(), "AB01");
    }

    @Test
    public void testCall()
    {
        assertThat(statement("CALL foo()")).isEqualTo(new Call(location(1, 1), qualifiedName(location(1, 6), "foo"), ImmutableList.of()));
        assertThat(statement("CALL foo(123, a => 1, b => 'go', 456)")).isEqualTo(
                new Call(location(1, 1), qualifiedName(location(1, 6), "foo"), ImmutableList.of(
                        new CallArgument(location(1, 10), new LongLiteral(location(1, 10), "123")),
                        new CallArgument(location(1, 15), "a", new LongLiteral(location(1, 20), "1")),
                        new CallArgument(location(1, 23), "b", new StringLiteral(location(1, 28), "go")),
                        new CallArgument(location(1, 34), new LongLiteral(location(1, 34), "456")))));
    }

    @Test
    public void testPrepare()
    {
        assertThat(statement("PREPARE myquery FROM select * from foo")).isEqualTo(
                new Prepare(location(1, 1),
                        new Identifier(location(1, 9), "myquery", false),
                        simpleQuery(location(1, 22),
                                selectList(location(1, 22), new AllColumns(location(1, 29), Optional.empty(), ImmutableList.of())),
                                new Table(location(1, 36), qualifiedName(location(1, 36), "foo")))));
    }

    @Test
    public void testPrepareWithParameters() {
        assertThat(statement("PREPARE myquery FROM SELECT ?, ? FROM foo")).isEqualTo(
                new Prepare(identifier("myquery"), simpleQuery(
                        selectList(new Parameter(0), new Parameter(1)),
                        table(QualifiedName.of("foo")))));

        assertThat(statement("PREPARE myquery FROM SELECT * FROM foo LIMIT ?")).isEqualTo(new Prepare(identifier("myquery"), simpleQuery(
                selectList(new AllColumns()),
                table(QualifiedName.of("foo")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new Limit(new Parameter(0))))));

        assertThat(statement("PREPARE myquery FROM SELECT ?, ? FROM foo LIMIT ?")).isEqualTo(new Prepare(identifier("myquery"), simpleQuery(
                selectList(new Parameter(0), new Parameter(1)),
                table(QualifiedName.of("foo")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new Limit(new Parameter(2))))));

        assertThat(statement("PREPARE myquery FROM SELECT ? FROM foo FETCH FIRST ? ROWS ONLY")).isEqualTo(new Prepare(identifier("myquery"), simpleQuery(
                selectList(new Parameter(0)),
                table(QualifiedName.of("foo")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new FetchFirst(new Parameter(1))))));

        assertThat(statement("PREPARE myquery FROM SELECT ?, ? FROM foo FETCH NEXT ? ROWS WITH TIES")).isEqualTo(new Prepare(identifier("myquery"), simpleQuery(
                selectList(new Parameter(0), new Parameter(1)),
                table(QualifiedName.of("foo")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new FetchFirst(new Parameter(2), true)))));

        assertThat(statement("PREPARE myquery FROM SELECT ?, ? FROM foo OFFSET ? ROWS")).isEqualTo(new Prepare(identifier("myquery"), simpleQuery(
                selectList(new Parameter(0), new Parameter(1)),
                table(QualifiedName.of("foo")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new Offset(new Parameter(2))),
                Optional.empty())));

        assertThat(statement("PREPARE myquery FROM SELECT ? FROM foo OFFSET ? ROWS LIMIT ?")).isEqualTo(new Prepare(identifier("myquery"), simpleQuery(
                selectList(new Parameter(0)),
                table(QualifiedName.of("foo")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new Offset(new Parameter(1))),
                Optional.of(new Limit(new Parameter(2))))));

        assertThat(statement("PREPARE myquery FROM SELECT ? FROM foo OFFSET ? ROWS FETCH FIRST ? ROWS WITH TIES")).isEqualTo(new Prepare(identifier("myquery"), simpleQuery(
                selectList(new Parameter(0)),
                table(QualifiedName.of("foo")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new Offset(new Parameter(1))),
                Optional.of(new FetchFirst(new Parameter(2), true)))));
    }

    @Test
    public void testDeallocatePrepare() {
        assertThat(statement("DEALLOCATE PREPARE myquery")).isEqualTo(new Deallocate(identifier("myquery")));
    }

    @Test
    public void testExecute() {
        assertThat(statement("EXECUTE myquery")).isEqualTo(new Execute(identifier("myquery"), emptyList()));
    }

    @Test
    public void testExecuteWithUsing() {
        assertThat(statement("EXECUTE myquery USING 1, 'abc', ARRAY ['hello']")).isEqualTo(new Execute(identifier("myquery"), ImmutableList.of(new LongLiteral("1"), new StringLiteral("abc"), new ArrayConstructor(ImmutableList.of(new StringLiteral("hello"))))));
    }

    @Test
    public void testExists() {
        assertThat(statement("SELECT EXISTS(SELECT 1)")).isEqualTo(simpleQuery(selectList(exists(simpleQuery(selectList(new LongLiteral("1")))))));

        assertThat(statement("SELECT EXISTS(SELECT 1) = EXISTS(SELECT 2)")).isEqualTo(simpleQuery(
                selectList(new ComparisonExpression(
                        ComparisonExpression.Operator.EQUAL,
                        exists(simpleQuery(selectList(new LongLiteral("1")))),
                        exists(simpleQuery(selectList(new LongLiteral("2"))))))));

        assertThat(statement("SELECT NOT EXISTS(SELECT 1) = EXISTS(SELECT 2)")).isEqualTo(simpleQuery(
                selectList(
                        new NotExpression(
                                new ComparisonExpression(
                                        ComparisonExpression.Operator.EQUAL,
                                        exists(simpleQuery(selectList(new LongLiteral("1")))),
                                        exists(simpleQuery(selectList(new LongLiteral("2")))))))));

        assertThat(statement("SELECT (NOT EXISTS(SELECT 1)) = EXISTS(SELECT 2)")).isEqualTo(simpleQuery(
                selectList(
                        new ComparisonExpression(
                                ComparisonExpression.Operator.EQUAL,
                                new NotExpression(exists(simpleQuery(selectList(new LongLiteral("1"))))),
                                exists(simpleQuery(selectList(new LongLiteral("2"))))))));
    }

    private static ExistsPredicate exists(Query query)
    {
        return new ExistsPredicate(new SubqueryExpression(query));
    }

    @Test
    public void testShowStats()
    {
        String[] tableNames = {"t", "s.t", "c.s.t"};

        for (String fullName : tableNames) {
            QualifiedName qualifiedName = makeQualifiedName(fullName);
            assertThat(statement(format("SHOW STATS FOR %s", qualifiedName))).isEqualTo(new ShowStats(new Table(qualifiedName)));
        }
    }

    @Test
    public void testShowStatsForQuery()
    {
        String[] tableNames = {"t", "s.t", "c.s.t"};

        for (String fullName : tableNames) {
            QualifiedName qualifiedName = makeQualifiedName(fullName);
            assertThat(statement(format("SHOW STATS FOR (SELECT * FROM %s)", qualifiedName))).isEqualTo(createShowStats(qualifiedName, ImmutableList.of(new AllColumns()), Optional.empty()));
            assertThat(statement(format("SHOW STATS FOR (SELECT * FROM %s WHERE field > 0)", qualifiedName))).isEqualTo(createShowStats(qualifiedName,
                    ImmutableList.of(new AllColumns()),
                    Optional.of(
                            new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN,
                                    new Identifier("field"),
                                    new LongLiteral("0")))));
            assertThat(statement(format("SHOW STATS FOR (SELECT * FROM %s WHERE field > 0 or field < 0)", qualifiedName))).isEqualTo(createShowStats(qualifiedName,
                    ImmutableList.of(new AllColumns()),
                    Optional.of(
                            new LogicalBinaryExpression(LogicalBinaryExpression.Operator.OR,
                                    new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN,
                                            new Identifier("field"),
                                            new LongLiteral("0")),
                                    new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN,
                                            new Identifier("field"),
                                            new LongLiteral("0"))))));
        }
    }

    private static ShowStats createShowStats(QualifiedName name, List<SelectItem> selects, Optional<Expression> where)
    {
        return new ShowStats(
                new TableSubquery(simpleQuery(new Select(false, selects),
                        new Table(name),
                        where,
                        Optional.empty())));
    }

    @Test
    public void testDescribeOutput() {
        assertThat(statement("DESCRIBE OUTPUT myquery")).isEqualTo(new DescribeOutput(identifier("myquery")));
    }

    @Test
    public void testDescribeInput() {
        assertThat(statement("DESCRIBE INPUT myquery")).isEqualTo(new DescribeInput(identifier("myquery")));
    }

    @Test
    public void testAggregationFilter() {
        assertThat(statement("SELECT SUM(x) FILTER (WHERE x > 4)")).isEqualTo(simpleQuery(selectList(
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("SUM"),
                        Optional.empty(),
                        Optional.of(new ComparisonExpression(
                                ComparisonExpression.Operator.GREATER_THAN,
                                new Identifier("x"),
                                new LongLiteral("4"))),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(new Identifier("x"))))));
    }

    @Test
    public void testQuantifiedComparison()
    {
        assertExpression("col1 < ANY (SELECT col2 FROM table1)",
                new QuantifiedComparisonExpression(
                        ComparisonExpression.Operator.LESS_THAN,
                        QuantifiedComparisonExpression.Quantifier.ANY,
                        identifier("col1"),
                        new SubqueryExpression(simpleQuery(selectList(new SingleColumn(identifier("col2"))), table(QualifiedName.of("table1"))))));
        assertExpression("col1 = ALL (VALUES ROW(1), ROW(2))",
                new QuantifiedComparisonExpression(
                        ComparisonExpression.Operator.EQUAL,
                        QuantifiedComparisonExpression.Quantifier.ALL,
                        identifier("col1"),
                        new SubqueryExpression(query(values(row(new LongLiteral("1")), row(new LongLiteral("2")))))));
        assertExpression("col1 >= SOME (SELECT 10)",
                new QuantifiedComparisonExpression(
                        ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
                        QuantifiedComparisonExpression.Quantifier.SOME,
                        identifier("col1"),
                        new SubqueryExpression(simpleQuery(selectList(new LongLiteral("10"))))));
    }

    @Test
    public void testAggregationWithOrderBy() {
        assertExpression("array_agg(x ORDER BY x DESC)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("array_agg"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new OrderBy(ImmutableList.of(new SortItem(identifier("x"), DESCENDING, UNDEFINED)))),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(identifier("x"))));
        assertThat(statement("SELECT array_agg(x ORDER BY t.y) FROM t")).isEqualTo(simpleQuery(
                selectList(new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("array_agg"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new OrderBy(ImmutableList.of(new SortItem(new DereferenceExpression(new Identifier("t"), identifier("y")), ASCENDING, UNDEFINED)))),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(new Identifier("x")))),
                table(QualifiedName.of("t"))));
    }

    @Test
    public void testCreateRole() {
        assertThat(statement("CREATE ROLE role")).isEqualTo(new CreateRole(new Identifier("role"), Optional.empty()));
        assertThat(statement("CREATE ROLE role1 WITH ADMIN admin")).isEqualTo(new CreateRole(
                new Identifier("role1"),
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("admin")))))));
        assertThat(statement("CREATE ROLE \"role\" WITH ADMIN \"admin\"")).isEqualTo(new CreateRole(
                new Identifier("role"),
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("admin")))))));
        assertThat(statement("CREATE ROLE \"ro le\" WITH ADMIN \"ad min\"")).isEqualTo(new CreateRole(
                new Identifier("ro le"),
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("ad min")))))));
        assertThat(statement("CREATE ROLE \"!@#$%^&*'\" WITH ADMIN \"ад\"\"мін\"")).isEqualTo(new CreateRole(
                new Identifier("!@#$%^&*'"),
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("ад\"мін")))))));
        assertThat(statement("CREATE ROLE role2 WITH ADMIN USER admin1")).isEqualTo(new CreateRole(
                new Identifier("role2"),
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("admin1")))))));
        assertThat(statement("CREATE ROLE role2 WITH ADMIN ROLE role1")).isEqualTo(new CreateRole(
                new Identifier("role2"),
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role1")))))));
        assertThat(statement("CREATE ROLE role2 WITH ADMIN CURRENT_USER")).isEqualTo(new CreateRole(
                new Identifier("role2"),
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.CURRENT_USER,
                        Optional.empty()))));
        assertThat(statement("CREATE ROLE role2 WITH ADMIN CURRENT_ROLE")).isEqualTo(new CreateRole(
                new Identifier("role2"),
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.CURRENT_ROLE,
                        Optional.empty()))));
    }

    @Test
    public void testDropRole() {
        assertThat(statement("DROP ROLE role")).isEqualTo(new DropRole(new Identifier("role")));
        assertThat(statement("DROP ROLE \"role\"")).isEqualTo(new DropRole(new Identifier("role")));
        assertThat(statement("DROP ROLE \"ro le\"")).isEqualTo(new DropRole(new Identifier("ro le")));
        assertThat(statement("DROP ROLE \"!@#$%^&*'ад\"\"мін\"")).isEqualTo(new DropRole(new Identifier("!@#$%^&*'ад\"мін")));
    }

    @Test
    public void testGrantRoles() {
        assertThat(statement("GRANT role1 TO user1")).isEqualTo(new GrantRoles(
                ImmutableSet.of(new Identifier("role1")),
                ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1"))),
                false,
                Optional.empty()));
        assertThat(statement("GRANT role1, role2, role3 TO user1, USER user2, ROLE role4 WITH ADMIN OPTION")).isEqualTo(new GrantRoles(
                ImmutableSet.of(new Identifier("role1"), new Identifier("role2"), new Identifier("role3")),
                ImmutableSet.of(
                        new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1")),
                        new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("user2")),
                        new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role4"))),
                true,
                Optional.empty()));
        assertThat(statement("GRANT role1 TO user1 WITH ADMIN OPTION GRANTED BY admin")).isEqualTo(new GrantRoles(
                ImmutableSet.of(new Identifier("role1")),
                ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1"))),
                true,
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("admin")))))));
        assertThat(statement("GRANT role1 TO USER user1 WITH ADMIN OPTION GRANTED BY USER admin")).isEqualTo(new GrantRoles(
                ImmutableSet.of(new Identifier("role1")),
                ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("user1"))),
                true,
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("admin")))))));
        assertThat(statement("GRANT role1 TO ROLE role2 WITH ADMIN OPTION GRANTED BY ROLE admin")).isEqualTo(new GrantRoles(
                ImmutableSet.of(new Identifier("role1")),
                ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                true,
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin")))))));
        assertThat(statement("GRANT role1 TO ROLE role2 GRANTED BY ROLE admin")).isEqualTo(new GrantRoles(
                ImmutableSet.of(new Identifier("role1")),
                ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                false,
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin")))))));
        assertThat(statement("GRANT \"role1\" TO ROLE \"role2\" GRANTED BY ROLE \"admin\"")).isEqualTo(new GrantRoles(
                ImmutableSet.of(new Identifier("role1")),
                ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                false,
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin")))))));
    }

    @Test
    public void testRevokeRoles() {
        assertThat(statement("REVOKE role1 FROM user1")).isEqualTo(new RevokeRoles(
                ImmutableSet.of(new Identifier("role1")),
                ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1"))),
                false,
                Optional.empty()));
        assertThat(statement("REVOKE ADMIN OPTION FOR role1, role2, role3 FROM user1, USER user2, ROLE role4")).isEqualTo(new RevokeRoles(
                ImmutableSet.of(new Identifier("role1"), new Identifier("role2"), new Identifier("role3")),
                ImmutableSet.of(
                        new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1")),
                        new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("user2")),
                        new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role4"))),
                true,
                Optional.empty()));
        assertThat(statement("REVOKE ADMIN OPTION FOR role1 FROM user1 GRANTED BY admin")).isEqualTo(new RevokeRoles(
                ImmutableSet.of(new Identifier("role1")),
                ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1"))),
                true,
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("admin")))))));
        assertThat(statement("REVOKE ADMIN OPTION FOR role1 FROM USER user1 GRANTED BY USER admin")).isEqualTo(new RevokeRoles(
                ImmutableSet.of(new Identifier("role1")),
                ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("user1"))),
                true,
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("admin")))))));
        assertThat(statement("REVOKE role1 FROM ROLE role2 GRANTED BY ROLE admin")).isEqualTo(new RevokeRoles(
                ImmutableSet.of(new Identifier("role1")),
                ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                false,
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin")))))));
        assertThat(statement("REVOKE \"role1\" FROM ROLE \"role2\" GRANTED BY ROLE \"admin\"")).isEqualTo(new RevokeRoles(
                ImmutableSet.of(new Identifier("role1")),
                ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                false,
                Optional.of(new GrantorSpecification(
                        GrantorSpecification.Type.PRINCIPAL,
                        Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin")))))));
    }

    @Test
    public void testSetRole() {
        assertThat(statement("SET ROLE ALL")).isEqualTo(new SetRole(SetRole.Type.ALL, Optional.empty()));
        assertThat(statement("SET ROLE NONE")).isEqualTo(new SetRole(SetRole.Type.NONE, Optional.empty()));
        assertThat(statement("SET ROLE role")).isEqualTo(new SetRole(SetRole.Type.ROLE, Optional.of(new Identifier("role"))));
        assertThat(statement("SET ROLE \"role\"")).isEqualTo(new SetRole(SetRole.Type.ROLE, Optional.of(new Identifier("role"))));
    }

    @Test
    public void testCreateMaterializedView() {
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t")));

        Optional<NodeLocation> location = Optional.empty();

        assertThat(statement("CREATE MATERIALIZED VIEW a AS SELECT * FROM t")).isEqualTo(new CreateMaterializedView(location,
                QualifiedName.of("a"), query, false, false, new ArrayList<>(), Optional.empty()));

        Query query2 = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("catalog2", "schema2", "tab")));
        assertThat(statement("CREATE OR REPLACE MATERIALIZED VIEW catalog.schema.matview COMMENT 'A simple materialized view'" +
                " AS SELECT * FROM catalog2.schema2.tab")).isEqualTo(new CreateMaterializedView(location, QualifiedName.of("catalog", "schema", "matview"), query2,
                true, false, new ArrayList<>(), Optional.of("A simple materialized view")));

        assertThat(statement("CREATE OR REPLACE MATERIALIZED VIEW catalog.schema.matview COMMENT 'A simple materialized view'" +
                " AS SELECT * FROM catalog2.schema2.tab")).isEqualTo(new CreateMaterializedView(location, QualifiedName.of("catalog", "schema", "matview"), query2,
                true, false, new ArrayList<>(), Optional.of("A simple materialized view")));

        List<Property> properties = ImmutableList.of(new Property(new Identifier("partitioned_by"),
                new ArrayConstructor(ImmutableList.of(new StringLiteral("dateint")))));

        assertThat(statement("CREATE OR REPLACE MATERIALIZED VIEW catalog.schema.matview COMMENT 'A simple materialized view'" +
                "WITH (partitioned_by = ARRAY ['dateint'])" +
                " AS SELECT * FROM catalog2.schema2.tab")).isEqualTo(new CreateMaterializedView(location, QualifiedName.of("catalog", "schema", "matview"), query2,
                true, false, properties, Optional.of("A simple materialized view")));

        Query query3 = new Query(Optional.of(new With(false, ImmutableList.of(
                new WithQuery(identifier("a"), simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("x"))), Optional.of(ImmutableList.of(identifier("t"), identifier("u")))),
                new WithQuery(identifier("b"), simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("a"))), Optional.empty())))),
                new Table(QualifiedName.of("b")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertThat(statement("CREATE OR REPLACE MATERIALIZED VIEW catalog.schema.matview COMMENT 'A partitioned materialized view' " +
                "WITH (partitioned_by = ARRAY ['dateint'])" +
                " AS WITH a (t, u) AS (SELECT * FROM x), b AS (SELECT * FROM a) TABLE b")).isEqualTo(new CreateMaterializedView(location, QualifiedName.of("catalog", "schema", "matview"), query3,
                true, false, properties, Optional.of("A partitioned materialized view")));
    }

    @Test
    public void testRefreshMaterializedView() {
        assertThat(statement("REFRESH MATERIALIZED VIEW test")).isEqualTo(new RefreshMaterializedView(Optional.empty(), QualifiedName.of("test")));

        assertThat(statement("REFRESH MATERIALIZED VIEW \"some name that contains space\"")).isEqualTo(new RefreshMaterializedView(Optional.empty(), QualifiedName.of("some name that contains space")));
    }

    @Test
    public void testDropMaterializedView() {
        assertThat(statement("DROP MATERIALIZED VIEW a")).isEqualTo(new DropMaterializedView(QualifiedName.of("a"), false));
        assertThat(statement("DROP MATERIALIZED VIEW a.b")).isEqualTo(new DropMaterializedView(QualifiedName.of("a", "b"), false));
        assertThat(statement("DROP MATERIALIZED VIEW a.b.c")).isEqualTo(new DropMaterializedView(QualifiedName.of("a", "b", "c"), false));

        assertThat(statement("DROP MATERIALIZED VIEW IF EXISTS a")).isEqualTo(new DropMaterializedView(QualifiedName.of("a"), true));
        assertThat(statement("DROP MATERIALIZED VIEW IF EXISTS a.b")).isEqualTo(new DropMaterializedView(QualifiedName.of("a", "b"), true));
        assertThat(statement("DROP MATERIALIZED VIEW IF EXISTS a.b.c")).isEqualTo(new DropMaterializedView(QualifiedName.of("a", "b", "c"), true));
    }

    @Test
    public void testNullTreatment()
    {
        assertExpression("lead(x, 1) ignore nulls over()",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("lead"),
                        Optional.of(new WindowSpecification(Optional.empty(), ImmutableList.of(), Optional.empty(), Optional.empty())),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.of(NullTreatment.IGNORE),
                        Optional.empty(),
                        ImmutableList.of(new Identifier("x"), new LongLiteral("1"))));
        assertExpression("lead(x, 1) respect nulls over()",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("lead"),
                        Optional.of(new WindowSpecification(Optional.empty(), ImmutableList.of(), Optional.empty(), Optional.empty())),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.of(NullTreatment.RESPECT),
                        Optional.empty(),
                        ImmutableList.of(new Identifier("x"), new LongLiteral("1"))));
    }

    @Test
    public void testProcessingMode()
    {
        assertExpression("RUNNING LAST(x, 1)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("LAST"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.of(new ProcessingMode(Optional.empty(), RUNNING)),
                        ImmutableList.of(new Identifier("x"), new LongLiteral("1"))));
        assertExpression("FINAL FIRST(x, 1)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("FIRST"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.of(new ProcessingMode(Optional.empty(), FINAL)),
                        ImmutableList.of(new Identifier("x"), new LongLiteral("1"))));
    }

    @Test
    public void testWindowSpecification()
    {
        assertExpression("rank() OVER someWindow",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("rank"),
                        Optional.of(new WindowReference(new Identifier("someWindow"))),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()));

        assertExpression("rank() OVER (someWindow PARTITION BY x ORDER BY y ROWS CURRENT ROW)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("rank"),
                        Optional.of(new WindowSpecification(
                                Optional.of(new Identifier("someWindow")),
                                ImmutableList.of(new Identifier("x")),
                                Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("y"), ASCENDING, UNDEFINED)))),
                                Optional.of(new WindowFrame(ROWS, new FrameBound(CURRENT_ROW), Optional.empty())))),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()));

        assertExpression("rank() OVER (PARTITION BY x ORDER BY y ROWS CURRENT ROW)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("rank"),
                        Optional.of(new WindowSpecification(
                                Optional.empty(),
                                ImmutableList.of(new Identifier("x")),
                                Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("y"), ASCENDING, UNDEFINED)))),
                                Optional.of(new WindowFrame(ROWS, new FrameBound(CURRENT_ROW), Optional.empty())))),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()));
    }

    @Test
    public void testWindowClause() {
        assertThat(statement("SELECT * FROM T WINDOW someWindow AS (PARTITION BY a), otherWindow AS (someWindow ORDER BY b)")).isEqualTo(simpleQuery(
                selectList(new AllColumns()),
                new Table(makeQualifiedName("T")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new WindowDefinition(
                                new Identifier("someWindow"),
                                new WindowSpecification(
                                        Optional.empty(),
                                        ImmutableList.of(new Identifier("a")),
                                        Optional.empty(),
                                        Optional.empty())),
                        new WindowDefinition(
                                new Identifier("otherWindow"),
                                new WindowSpecification(
                                        Optional.of(new Identifier("someWindow")),
                                        ImmutableList.of(),
                                        Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("b"), ASCENDING, UNDEFINED)))),
                                        Optional.empty()))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));
    }

    public void testUpdate() {
        assertThat(statement("" +
                "UPDATE foo_table\n" +
                "    SET bar = 23, baz = 3.1415E0, bletch = 'barf'\n" +
                "WHERE (nothing = 'fun')")).isEqualTo(new Update(
                new NodeLocation(1, 1),
                table(QualifiedName.of("foo_table")),
                ImmutableList.of(
                        new UpdateAssignment(new Identifier("bar"), new LongLiteral("23")),
                        new UpdateAssignment(new Identifier("baz"), new DoubleLiteral("3.1415")),
                        new UpdateAssignment(new Identifier("bletch"), new StringLiteral("barf"))),
                Optional.of(new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new Identifier("nothing"), new StringLiteral("fun")))));
    }

    @Test
    public void testWherelessUpdate() {
        assertThat(statement("" +
                "UPDATE foo_table\n" +
                "    SET bar = 23")).isEqualTo(new Update(
                new NodeLocation(1, 1),
                table(QualifiedName.of("foo_table")),
                ImmutableList.of(
                        new UpdateAssignment(new Identifier("bar"), new LongLiteral("23"))),
                Optional.empty()));
    }

    private static QualifiedName makeQualifiedName(String tableName)
    {
        List<Identifier> parts = Splitter.on('.').splitToList(tableName).stream()
                .map(Identifier::new)
                .collect(toImmutableList());
        return QualifiedName.of(parts);
    }

    /**
     * @deprecated use {@link ParserAssert#statement(String)} instead
     */
    @Deprecated
    private static void assertStatement(String query, Statement expected)
    {
        assertParsed(query, expected, SQL_PARSER.createStatement(query, new ParsingOptions()));
        assertFormattedSql(SQL_PARSER, expected);
    }

    /**
     * @deprecated use {@link ParserAssert#statement(String)} instead
     */
    @Deprecated
    private static void assertInvalidStatement(String statement, String expectedErrorMessageRegex)
    {
        assertThatThrownBy(() -> SQL_PARSER.createStatement(statement, new ParsingOptions()))
                .isInstanceOfSatisfying(ParsingException.class, e -> assertTrue(e.getErrorMessage().matches(expectedErrorMessageRegex)));
    }

    /**
     * @deprecated use {@link ParserAssert#expression(String)} instead
     */
    @Deprecated
    private static void assertExpression(String expression, Expression expected)
    {
        requireNonNull(expression, "expression is null");
        requireNonNull(expected, "expected is null");
        assertParsed(expression, expected, SQL_PARSER.createExpression(expression, new ParsingOptions(AS_DECIMAL)));
    }

    private static void assertParsed(String input, Node expected, Node parsed)
    {
        if (!parsed.equals(expected)) {
            fail(format("expected\n\n%s\n\nto parse as\n\n%s\n\nbut was\n\n%s\n",
                    indent(input),
                    indent(formatSql(expected)),
                    indent(formatSql(parsed))));
        }
    }

    private static void assertInvalidExpression(String expression, String expectedErrorMessageRegex)
    {
        assertThatThrownBy(() -> createExpression(expression))
                .isInstanceOfSatisfying(ParsingException.class, e -> assertTrue(e.getErrorMessage().matches(expectedErrorMessageRegex)));
    }

    private static String indent(String value)
    {
        String indent = "    ";
        return indent + value.trim().replaceAll("\n", "\n" + indent);
    }

    private static Expression createExpression(String expression)
    {
        return SQL_PARSER.createExpression(expression, new ParsingOptions());
    }
}
