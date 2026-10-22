using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Chef.DbAccess.Metadata;
using Chef.DbAccess.SqlExpressions;
using Chef.DbAccess.SqlGeneration;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.SqlGeneration
{
    [TestClass]
    public class SqlGeneratorTest
    {
        private static readonly EntityMetadata MemberEntity = EntityMetadata.For<Member>();

        private readonly TableExpression table = TableExpression.For(MemberEntity, "m");

        private ColumnExpression Id => this.table.Column(MemberEntity.FindColumn(nameof(Member.Id)));

        private ColumnExpression Name => this.table.Column(MemberEntity.FindColumn(nameof(Member.Name)));

        private ColumnExpression Age => this.table.Column(MemberEntity.FindColumn(nameof(Member.Age)));

        [TestMethod]
        public void Select_columns_omits_alias_when_same_as_column_name()
        {
            var sql = Generate(this.Select(where: null));

            sql.Sql.Should().Be("SELECT \"m\".\"No\" AS \"Id\", \"m\".\"Name\"\nFROM \"tbl_member\" \"m\"");
            sql.Parameters.Should().BeEmpty();
        }

        [TestMethod]
        public void Table_schema_is_delimited_when_present()
        {
            var archive = new TableExpression("tbl_member", "archive", "a");
            var select = new SelectExpression(new[] { new ProjectionExpression(new ColumnExpression(archive, "No", typeof(int)), "No") }, new[] { archive });

            Generate(select).Sql.Should().Be("SELECT \"a\".\"No\"\nFROM \"archive\".\"tbl_member\" \"a\"");
        }

        [TestMethod]
        public void Where_parenthesizes_nested_binary_operands()
        {
            var predicate = And(
                Or(Binary(SqlBinaryOperator.Equal, this.Id, Parameter("Id_0", 1)), Binary(SqlBinaryOperator.Equal, this.Id, Parameter("Id_1", 2))),
                Binary(SqlBinaryOperator.GreaterThan, this.Age, Parameter("Age_0", 18)));

            Generate(this.Select(predicate)).Sql.Should().EndWith("\nWHERE ((\"m\".\"No\" = @Id_0) OR (\"m\".\"No\" = @Id_1)) AND (\"m\".\"Age\" > @Age_0)");
        }

        [TestMethod]
        public void Comparison_and_arithmetic_operators_are_mapped()
        {
            var cases = new[]
                        {
                            (SqlBinaryOperator.NotEqual, "<>"),
                            (SqlBinaryOperator.LessThan, "<"),
                            (SqlBinaryOperator.LessThanOrEqual, "<="),
                            (SqlBinaryOperator.GreaterThanOrEqual, ">="),
                            (SqlBinaryOperator.Subtract, "-"),
                            (SqlBinaryOperator.Multiply, "*"),
                            (SqlBinaryOperator.Divide, "/"),
                            (SqlBinaryOperator.Modulo, "%"),
                            (SqlBinaryOperator.BitwiseAnd, "&"),
                            (SqlBinaryOperator.BitwiseOr, "|"),
                            (SqlBinaryOperator.BitwiseXor, "^")
                        };

            foreach (var (@operator, symbol) in cases)
            {
                Generate(this.Select(Binary(@operator, this.Age, Constant(1)))).Sql.Should().EndWith($"\nWHERE \"m\".\"Age\" {symbol} 1", because: @operator.ToString());
            }
        }

        [TestMethod]
        public void Arithmetic_inside_comparison_is_parenthesized()
        {
            var predicate = Binary(SqlBinaryOperator.GreaterThan, new SqlBinaryExpression(SqlBinaryOperator.Add, this.Age, Constant(1), typeof(int)), Parameter("p0", 18));

            Generate(this.Select(predicate)).Sql.Should().EndWith("\nWHERE (\"m\".\"Age\" + 1) > @p0");
        }

        [TestMethod]
        public void Like_with_string_concatenation_uses_ansi_operator()
        {
            var pattern = new SqlBinaryExpression(SqlBinaryOperator.Add, Parameter("Name_0", "J"), new SqlConstantExpression("%", typeof(string)), typeof(string));

            Generate(this.Select(new LikeExpression(this.Name, pattern))).Sql.Should().EndWith("\nWHERE \"m\".\"Name\" LIKE (@Name_0 || '%')");
        }

        [TestMethod]
        public void Like_with_escape_char()
        {
            Generate(this.Select(new LikeExpression(this.Name, Parameter("Name_0", "50\\%"), '\\'))).Sql.Should().EndWith("\nWHERE \"m\".\"Name\" LIKE @Name_0 ESCAPE '\\'");
        }

        [TestMethod]
        public void Like_inside_and_is_parenthesized()
        {
            var predicate = And(Binary(SqlBinaryOperator.Equal, this.Age, Constant(1)), new LikeExpression(this.Name, Parameter("Name_0", "J")));

            Generate(this.Select(predicate)).Sql.Should().EndWith("\nWHERE (\"m\".\"Age\" = 1) AND (\"m\".\"Name\" LIKE @Name_0)");
        }

        [TestMethod]
        public void Case_with_multiple_when_clauses_and_without_else()
        {
            var @case = new CaseExpression(
                new[]
                {
                    new CaseWhenClause(Binary(SqlBinaryOperator.GreaterThan, this.Age, Constant(60)), Constant(2)),
                    new CaseWhenClause(Binary(SqlBinaryOperator.GreaterThan, this.Age, Constant(18)), Constant(1))
                },
                null,
                typeof(int));

            Generate(this.Select(Binary(SqlBinaryOperator.Equal, @case, Constant(1))))
                .Sql.Should()
                .EndWith("\nWHERE CASE WHEN \"m\".\"Age\" > 60 THEN 2 WHEN \"m\".\"Age\" > 18 THEN 1 END = 1");
        }

        [TestMethod]
        public void Unary_operators()
        {
            Generate(this.Select(new SqlUnaryExpression(SqlUnaryOperator.IsNull, this.Name, typeof(bool)))).Sql.Should().EndWith("\nWHERE \"m\".\"Name\" IS NULL");
            Generate(this.Select(new SqlUnaryExpression(SqlUnaryOperator.IsNotNull, this.Name, typeof(bool)))).Sql.Should().EndWith("\nWHERE \"m\".\"Name\" IS NOT NULL");

            Generate(this.Select(new SqlUnaryExpression(SqlUnaryOperator.Not, Binary(SqlBinaryOperator.Equal, this.Age, Constant(1)), typeof(bool))))
                .Sql.Should()
                .EndWith("\nWHERE NOT (\"m\".\"Age\" = 1)");

            Generate(this.Select(Binary(SqlBinaryOperator.Equal, new SqlUnaryExpression(SqlUnaryOperator.Negate, this.Age, typeof(int)), Constant(1))))
                .Sql.Should()
                .EndWith("\nWHERE -\"m\".\"Age\" = 1");
        }

        [TestMethod]
        public void In_and_not_in()
        {
            var values = new[] { Parameter("Id_0", 1), Parameter("Id_1", 2) };

            Generate(this.Select(new InExpression(this.Id, values))).Sql.Should().EndWith("\nWHERE \"m\".\"No\" IN (@Id_0, @Id_1)");
            Generate(this.Select(new InExpression(this.Id, values, isNegated: true))).Sql.Should().EndWith("\nWHERE \"m\".\"No\" NOT IN (@Id_0, @Id_1)");
        }

        [TestMethod]
        public void Empty_in_is_always_false_and_empty_not_in_is_always_true()
        {
            Generate(this.Select(new InExpression(this.Id, Array.Empty<SqlExpression>()))).Sql.Should().EndWith("\nWHERE 1 = 0");
            Generate(this.Select(new InExpression(this.Id, Array.Empty<SqlExpression>(), isNegated: true))).Sql.Should().EndWith("\nWHERE 1 = 1");
        }

        [TestMethod]
        public void Function_call()
        {
            var length = new SqlFunctionExpression("LENGTH", new[] { this.Name }, typeof(int));

            Generate(this.Select(Binary(SqlBinaryOperator.GreaterThan, length, Constant(3)))).Sql.Should().EndWith("\nWHERE LENGTH(\"m\".\"Name\") > 3");
        }

        [TestMethod]
        public void Distinct_order_by_and_paging()
        {
            var select = new SelectExpression(
                new[] { new ProjectionExpression(this.Name, "Name") },
                new[] { this.table },
                orderings: new[] { new OrderingExpression(this.Age, true), new OrderingExpression(this.Id, false) },
                offset: Constant(20),
                limit: Constant(10),
                isDistinct: true);

            Generate(select)
                .Sql.Should()
                .Be("SELECT DISTINCT \"m\".\"Name\"\nFROM \"tbl_member\" \"m\"\nORDER BY \"m\".\"Age\" ASC, \"m\".\"No\" DESC\nOFFSET 20 ROWS\nFETCH NEXT 10 ROWS ONLY");
        }

        [TestMethod]
        public void Limit_without_offset_and_offset_without_limit()
        {
            Generate(new SelectExpression(new[] { new ProjectionExpression(this.Name, "Name") }, new[] { this.table }, limit: Constant(10)))
                .Sql.Should()
                .EndWith("\nFETCH FIRST 10 ROWS ONLY");

            Generate(new SelectExpression(new[] { new ProjectionExpression(this.Name, "Name") }, new[] { this.table }, offset: Constant(20)))
                .Sql.Should()
                .EndWith("\nOFFSET 20 ROWS");
        }

        [TestMethod]
        public void Constants_are_formatted_with_invariant_culture_and_escaped()
        {
            Generate(this.Select(Binary(SqlBinaryOperator.Equal, this.Name, new SqlConstantExpression("O'Brien", typeof(string))))).Sql.Should().EndWith("= 'O''Brien'");
            Generate(this.Select(Binary(SqlBinaryOperator.Equal, this.Age, new SqlConstantExpression(1.5m, typeof(decimal))))).Sql.Should().EndWith("= 1.5");
            Generate(this.Select(Binary(SqlBinaryOperator.Equal, this.Age, new SqlConstantExpression(true, typeof(bool))))).Sql.Should().EndWith("= TRUE");
            Generate(this.Select(Binary(SqlBinaryOperator.Equal, this.Age, new SqlConstantExpression(null, typeof(int?))))).Sql.Should().EndWith("= NULL");
        }

        [TestMethod]
        public void Unsupported_constant_type_throws()
        {
            Action act = () => Generate(this.Select(Binary(SqlBinaryOperator.Equal, this.Age, new SqlConstantExpression(DateTime.Now, typeof(DateTime)))));

            act.Should().Throw<NotSupportedException>();
        }

        [TestMethod]
        public void Identifier_with_quote_is_escaped()
        {
            var odd = new TableExpression("odd\"table", null, "o");
            var select = new SelectExpression(new[] { new ProjectionExpression(new ColumnExpression(odd, "No", typeof(int)), "No") }, new[] { odd });

            Generate(select).Sql.Should().EndWith("FROM \"odd\"\"table\" \"o\"");
        }

        [TestMethod]
        public void Parameters_are_collected_in_order_without_duplicates()
        {
            var age = Parameter("Age_0", 18);

            var predicate = And(
                Binary(SqlBinaryOperator.GreaterThan, this.Age, age),
                And(Binary(SqlBinaryOperator.Equal, this.Name, Parameter("Name_0", "Johnny")), Binary(SqlBinaryOperator.LessThan, this.Id, age)));

            Generate(this.Select(predicate)).Parameters.Select(p => p.Name).Should().Equal("Age_0", "Name_0");
        }

        [TestMethod]
        public void Same_parameter_name_with_different_values_throws()
        {
            var predicate = And(Binary(SqlBinaryOperator.Equal, this.Id, Parameter("p0", 1)), Binary(SqlBinaryOperator.Equal, this.Age, Parameter("p0", 2)));

            Action act = () => Generate(this.Select(predicate));

            act.Should().Throw<InvalidOperationException>();
        }

        [TestMethod]
        public void Generator_can_be_reused_without_leaking_state()
        {
            var generator = new SqlGenerator();

            generator.Generate(this.Select(Binary(SqlBinaryOperator.Equal, this.Id, Parameter("Id_0", 1))));
            var second = generator.Generate(this.Select(where: null));

            second.Sql.Should().NotContain("WHERE");
            second.Parameters.Should().BeEmpty();
        }

        [TestMethod]
        public void Inner_and_left_joins_are_written_on_their_own_lines()
        {
            var departmentEntity = EntityMetadata.For<Department>();
            var department = TableExpression.For(departmentEntity, "d");
            var manager = TableExpression.For(MemberEntity, "mgr");

            var select = new SelectExpression(
                new[] { new ProjectionExpression(this.Name, "Name"), new ProjectionExpression(department.Column(departmentEntity.FindColumn(nameof(Department.Name))), "DepartmentName") },
                new TableExpressionBase[]
                {
                    this.table,
                    new JoinExpression(
                        JoinType.Inner,
                        department,
                        Binary(SqlBinaryOperator.Equal, this.table.Column(MemberEntity.FindColumn(nameof(Member.DepartmentId))), department.Column(departmentEntity.FindColumn(nameof(Department.Id))))),
                    new JoinExpression(
                        JoinType.Left,
                        manager,
                        Binary(SqlBinaryOperator.Equal, this.table.Column(MemberEntity.FindColumn(nameof(Member.ManagerId))), manager.Column(MemberEntity.FindColumn(nameof(Member.Id)))))
                });

            Generate(select)
                .Sql.Should()
                .Be(
                    "SELECT \"m\".\"Name\", \"d\".\"Name\" AS \"DepartmentName\"\n" +
                    "FROM \"tbl_member\" \"m\"\n" +
                    "INNER JOIN \"tbl_department\" \"d\" ON \"m\".\"DepartmentId\" = \"d\".\"Id\"\n" +
                    "LEFT JOIN \"tbl_member\" \"mgr\" ON \"m\".\"ManagerId\" = \"mgr\".\"No\"");
        }

        [TestMethod]
        public void Join_predicate_parameters_are_collected_before_where_parameters()
        {
            var departmentEntity = EntityMetadata.For<Department>();
            var department = TableExpression.For(departmentEntity, "d");

            var on = new SqlBinaryExpression(
                SqlBinaryOperator.AndAlso,
                Binary(SqlBinaryOperator.Equal, this.table.Column(MemberEntity.FindColumn(nameof(Member.DepartmentId))), department.Column(departmentEntity.FindColumn(nameof(Department.Id)))),
                Binary(SqlBinaryOperator.Equal, department.Column(departmentEntity.FindColumn(nameof(Department.Name))), Parameter("Name_0", "Sales")),
                typeof(bool));

            var select = new SelectExpression(
                new[] { new ProjectionExpression(this.Name, "Name") },
                new TableExpressionBase[] { this.table, new JoinExpression(JoinType.Inner, department, on) },
                Binary(SqlBinaryOperator.GreaterThan, this.Age, Parameter("Age_0", 18)));

            Generate(select).Parameters.Select(p => p.Name).Should().Equal("Name_0", "Age_0");
        }

        [TestMethod]
        public void Tables_without_join_are_separated_by_comma()
        {
            var other = new TableExpression("tbl_department", null, "d");

            Generate(new SelectExpression(new[] { new ProjectionExpression(this.Name, "Name") }, new TableExpressionBase[] { this.table, other }))
                .Sql.Should()
                .EndWith("FROM \"tbl_member\" \"m\", \"tbl_department\" \"d\"");
        }

        private static GeneratedSql Generate(SelectExpression select)
        {
            return new SqlGenerator().Generate(select);
        }

        private static SqlBinaryExpression Binary(SqlBinaryOperator @operator, SqlExpression left, SqlExpression right)
        {
            return new SqlBinaryExpression(@operator, left, right, typeof(bool));
        }

        private static SqlBinaryExpression And(SqlExpression left, SqlExpression right)
        {
            return Binary(SqlBinaryOperator.AndAlso, left, right);
        }

        private static SqlBinaryExpression Or(SqlExpression left, SqlExpression right)
        {
            return Binary(SqlBinaryOperator.OrElse, left, right);
        }

        private static SqlParameterExpression Parameter(string name, object value)
        {
            return new SqlParameterExpression(name, value, value.GetType());
        }

        private static SqlConstantExpression Constant(int value)
        {
            return new SqlConstantExpression(value, typeof(int));
        }

        private SelectExpression Select(SqlExpression where)
        {
            return new SelectExpression(new[] { new ProjectionExpression(this.Id, "Id"), new ProjectionExpression(this.Name, "Name") }, new[] { this.table }, where);
        }

        [Table("tbl_member")]
        private class Member
        {
            [Column("No")]
            public int Id { get; set; }

            public string Name { get; set; }

            public int Age { get; set; }

            public int DepartmentId { get; set; }

            public int ManagerId { get; set; }
        }

        [Table("tbl_department")]
        private class Department
        {
            public int Id { get; set; }

            public string Name { get; set; }
        }
    }
}
