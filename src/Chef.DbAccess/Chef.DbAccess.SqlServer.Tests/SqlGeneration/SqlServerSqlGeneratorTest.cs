using System;
using Chef.DbAccess.Metadata;
using Chef.DbAccess.SqlExpressions;
using Chef.DbAccess.SqlGeneration;
using Chef.DbAccess.SqlServer.SqlGeneration;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.SqlGeneration
{
    // 放在命名空間內，才會優先於 Chef.DbAccess.SqlServer.Tests 底下整合測試用的 Member。
    using Member = Characterization.Member;

    /// <summary>
    ///     用新 AST 組出與 3.x 特徵化快照相同的查詢，確認 SQL Server 方言的輸出。
    ///     與 3.x 的差異都寫在各測試的註解裡（3.x 快照位於 Characterization/Snapshots/QueryCharacterizationTest）。
    /// </summary>
    [TestClass]
    public class SqlServerSqlGeneratorTest
    {
        private static readonly EntityMetadata MemberEntity = EntityMetadata.For<Member>();

        private readonly TableExpression table = TableExpression.For(MemberEntity, "mem_1");

        private ColumnExpression Id => this.Column(nameof(Member.Id));

        private ColumnExpression Name => this.Column(nameof(Member.Name));

        private ColumnExpression Age => this.Column(nameof(Member.Age));

        [TestMethod]
        public void StartsWith_OrderBy_Take_uses_top()
        {
            // 對應 3.x Query_StartsWith_OrderBy_Take。
            // 差異：數值參數用 @Age_0 而不是 Dapper 的 {=Age_0}；LIKE 右邊的串接多一層括號；結尾沒有分號。
            var predicate = And(
                Binary(SqlBinaryOperator.GreaterThanOrEqual, this.Age, Parameter("Age_0", 30)),
                Binary(
                    SqlBinaryOperator.Like,
                    this.Name,
                    new SqlBinaryExpression(SqlBinaryOperator.Add, Parameter("Name_0", "J"), new SqlConstantExpression("%", typeof(string)), typeof(string))));

            var select = new SelectExpression(
                new[] { this.Project(this.Name), this.Project(this.Age) },
                new[] { this.table },
                predicate,
                new[] { new OrderingExpression(this.Age, true) },
                limit: Constant(10));

            Generate(select)
                .Should()
                .Be(
                    "SELECT TOP(10) [mem_1].[Name], [mem_1].[Age]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "WHERE ([mem_1].[Age] >= @Age_0) AND ([mem_1].[Name] LIKE (@Name_0 + '%'))\n" +
                    "ORDER BY [mem_1].[Age] ASC");
        }

        [TestMethod]
        public void Skip_Take_uses_offset_fetch_without_top()
        {
            // 對應 3.x Query_Skip_Take，輸出相同（除了結尾分號）。
            var select = new SelectExpression(
                new[] { this.Project(this.Name), this.Project(this.Age) },
                new[] { this.table },
                orderings: new[] { new OrderingExpression(this.Id, true) },
                offset: Constant(20),
                limit: Constant(10));

            Generate(select)
                .Should()
                .Be(
                    "SELECT [mem_1].[Name], [mem_1].[Age]\n" +
                    "FROM [tbl_member] [mem_1] WITH (NOLOCK)\n" +
                    "ORDER BY [mem_1].[No] ASC\n" +
                    "OFFSET 20 ROWS\n" +
                    "FETCH NEXT 10 ROWS ONLY");
        }

        [TestMethod]
        public void Offset_without_ordering_adds_order_by_select_1()
        {
            // 差異：3.x 不補 ORDER BY，SQL Server 會回報語法錯誤。
            var select = new SelectExpression(new[] { this.Project(this.Name) }, new[] { this.table }, offset: Constant(20));

            Generate(select).Should().EndWith("\nORDER BY (SELECT 1)\nOFFSET 20 ROWS");
        }

        [TestMethod]
        public void No_lock_can_be_turned_off()
        {
            // 對應 3.x Query_Without_NoLock_When_Not_DirtyRead（IsDirtyRead = false）。
            var select = new SelectExpression(new[] { this.Project(this.Id) }, new[] { this.table }, Binary(SqlBinaryOperator.Equal, this.Id, Parameter("Id_0", 1)));

            new SqlServerSqlGenerator(withNoLock: false).Generate(select)
                .Sql.Should()
                .Be(
                    "SELECT [mem_1].[No] AS [Id]\n" +
                    "FROM [tbl_member] [mem_1]\n" +
                    "WHERE [mem_1].[No] = @Id_0");
        }

        [TestMethod]
        public void Null_checks_are_not_parenthesized()
        {
            // 對應 3.x Query_Null_Comparisons。
            // 差異：3.x 是 ([Phone] IS NULL) AND ([Birthday] IS NOT NULL)，且會多送兩個沒用到的 NULL 參數。
            var predicate = And(
                new SqlUnaryExpression(SqlUnaryOperator.IsNull, this.Column(nameof(Member.Phone)), typeof(bool)),
                new SqlUnaryExpression(SqlUnaryOperator.IsNotNull, this.Column(nameof(Member.Birthday)), typeof(bool)));

            var generated = new SqlServerSqlGenerator().Generate(new SelectExpression(new[] { this.Project(this.Name) }, new[] { this.table }, predicate));

            generated.Sql.Should().EndWith("\nWHERE [mem_1].[Phone] IS NULL AND [mem_1].[Birthday] IS NOT NULL");
            generated.Parameters.Should().BeEmpty();
        }

        [TestMethod]
        public void Distinct()
        {
            // 對應 3.x Query_Distinct（WHERE 以外的部分）。
            var select = new SelectExpression(new[] { this.Project(this.Name) }, new[] { this.table }, isDistinct: true);

            Generate(select).Should().Be("SELECT DISTINCT [mem_1].[Name]\nFROM [tbl_member] [mem_1] WITH (NOLOCK)");
        }

        [TestMethod]
        public void Bracket_in_identifier_is_escaped()
        {
            var odd = new TableExpression("odd]table", "dbo", "o");
            var select = new SelectExpression(new[] { new ProjectionExpression(new ColumnExpression(odd, "No", typeof(int)), "No") }, new[] { odd });

            Generate(select).Should().EndWith("FROM [dbo].[odd]]table] [o] WITH (NOLOCK)");
        }

        [TestMethod]
        public void String_concatenation_uses_plus()
        {
            var concat = new SqlBinaryExpression(SqlBinaryOperator.Add, this.Name, new SqlConstantExpression("!", typeof(string)), typeof(string));

            Generate(new SelectExpression(new[] { new ProjectionExpression(concat, "Greeting") }, new[] { this.table })).Should().StartWith("SELECT [mem_1].[Name] + '!' AS [Greeting]");
        }

        [TestMethod]
        public void Boolean_and_unicode_constants()
        {
            var predicate = And(
                Binary(SqlBinaryOperator.Equal, this.Column(nameof(Member.IsActive)), new SqlConstantExpression(true, typeof(bool))),
                Binary(SqlBinaryOperator.NotEqual, this.Name, new SqlConstantExpression("業務", typeof(string))));

            Generate(new SelectExpression(new[] { this.Project(this.Name) }, new[] { this.table }, predicate))
                .Should()
                .EndWith("\nWHERE ([mem_1].[IsActive] = 1) AND ([mem_1].[Name] <> N'業務')");
        }

        [TestMethod]
        public void Ascii_string_constant_has_no_unicode_prefix()
        {
            var predicate = Binary(SqlBinaryOperator.Equal, this.Name, new SqlConstantExpression("abc", typeof(string)));

            Generate(new SelectExpression(new[] { this.Project(this.Name) }, new[] { this.table }, predicate)).Should().EndWith("= 'abc'");
        }

        /// <summary>
        ///     使用預設建構子，所以用到它的測試同時驗證了 NOLOCK 預設開啟。
        /// </summary>
        private static string Generate(SelectExpression select)
        {
            return new SqlServerSqlGenerator().Generate(select).Sql;
        }

        private static SqlBinaryExpression Binary(SqlBinaryOperator @operator, SqlExpression left, SqlExpression right)
        {
            return new SqlBinaryExpression(@operator, left, right, typeof(bool));
        }

        private static SqlBinaryExpression And(SqlExpression left, SqlExpression right)
        {
            return Binary(SqlBinaryOperator.AndAlso, left, right);
        }

        private static SqlParameterExpression Parameter(string name, object value)
        {
            return new SqlParameterExpression(name, value, value.GetType());
        }

        private static SqlConstantExpression Constant(int value)
        {
            return new SqlConstantExpression(value, typeof(int));
        }

        private ColumnExpression Column(string propertyName)
        {
            return this.table.Column(MemberEntity.FindColumn(propertyName) ?? throw new ArgumentException(propertyName));
        }

        private ProjectionExpression Project(ColumnExpression column)
        {
            return new ProjectionExpression(column, column.Property.PropertyName);
        }
    }
}
