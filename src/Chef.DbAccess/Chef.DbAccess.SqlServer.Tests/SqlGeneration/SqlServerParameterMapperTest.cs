using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq;
using System.Reflection;
using Chef.DbAccess.Metadata;
using Chef.DbAccess.SqlExpressions;
using Chef.DbAccess.SqlGeneration;
using Chef.DbAccess.SqlServer.SqlGeneration;
using Dapper;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.SqlGeneration
{
    [TestClass]
    public class SqlServerParameterMapperTest
    {
        private static readonly EntityMetadata Entity = EntityMetadata.For<Customer>();

        [TestMethod]
        public void Varchar_column_uses_ansi_db_string_with_string_length()
        {
            // 與 3.x Query_Varchar_Column_Uses_DbString 的參數相同。
            var dbString = MapDbString(nameof(Customer.Phone), "0912345678");

            dbString.Value.Should().Be("0912345678");
            dbString.IsAnsi.Should().BeTrue();
            dbString.IsFixedLength.Should().BeFalse();
            dbString.Length.Should().Be(20);
        }

        [TestMethod]
        public void Char_nchar_and_nvarchar_columns()
        {
            var code = MapDbString(nameof(Customer.Code), "A1");
            code.IsAnsi.Should().BeTrue();
            code.IsFixedLength.Should().BeTrue();

            var region = MapDbString(nameof(Customer.Region), "台北");
            region.IsAnsi.Should().BeFalse();
            region.IsFixedLength.Should().BeTrue();

            var nickname = MapDbString(nameof(Customer.Nickname), "Johnny");
            nickname.IsAnsi.Should().BeFalse();
            nickname.IsFixedLength.Should().BeFalse();
        }

        [TestMethod]
        public void Other_string_type_names_fall_back_to_unicode_variable_length()
        {
            var memo = MapDbString(nameof(Customer.Memo), "note");

            memo.IsAnsi.Should().BeFalse();
            memo.IsFixedLength.Should().BeFalse();
        }

        [TestMethod]
        public void Length_is_minus_one_without_string_length()
        {
            MapDbString(nameof(Customer.Email), "a@b.c").Length.Should().Be(-1);
        }

        [TestMethod]
        public void Type_name_is_case_insensitive()
        {
            MapDbString(nameof(Customer.Email), "a@b.c").IsAnsi.Should().BeTrue();
        }

        [TestMethod]
        public void Type_name_with_length_uses_base_type()
        {
            // 差異：3.x 把 varchar(30) 當成 nvarchar（IsAnsi = false）。
            var address = MapDbString(nameof(Customer.Address), "Taipei");

            address.IsAnsi.Should().BeTrue();
            address.Length.Should().Be(30);
        }

        [TestMethod]
        public void String_without_type_name_is_left_to_dapper()
        {
            Map(nameof(Customer.Name), "Johnny").Should().Be(("Johnny", (DbType?)null));
        }

        [TestMethod]
        public void Parameter_without_property_is_left_to_dapper()
        {
            SqlServerParameterMapper.Map(new SqlParameterExpression("p0", "abc", typeof(string))).Should().Be(("abc", (DbType?)null));
        }

        [TestMethod]
        public void Null_string_is_left_to_dapper()
        {
            Map(nameof(Customer.Phone), null).Should().Be(((object)null, (DbType?)null));
        }

        [TestMethod]
        public void Date_and_time_type_names_specify_db_type()
        {
            // 差異：3.x 會把 DateTime 強制轉型成 string 而丟出 InvalidCastException。
            var joinedAt = new DateTime(2020, 1, 2, 3, 4, 5).AddTicks(1234567);

            Map(nameof(Customer.JoinedAt), joinedAt).Should().Be((joinedAt, DbType.DateTime2));
            Map(nameof(Customer.BirthDate), joinedAt.Date).Should().Be((joinedAt.Date, DbType.Date));
            Map(nameof(Customer.OpenAt), TimeSpan.FromHours(9)).Should().Be((TimeSpan.FromHours(9), DbType.Time));
        }

        [TestMethod]
        public void Datetime2_with_precision_and_null_value_still_specify_db_type()
        {
            Map(nameof(Customer.PreciseAt), null).Should().Be(((object)null, DbType.DateTime2));
        }

        [TestMethod]
        public void Datetime_type_name_is_left_to_dapper()
        {
            var createdAt = new DateTime(2020, 1, 2);

            Map(nameof(Customer.CreatedAt), createdAt).Should().Be((createdAt, (DbType?)null));
        }

        [TestMethod]
        public void Dynamic_parameters_produce_expected_sql_parameter_types()
        {
            var table = TableExpression.For(Entity, "c");
            var phone = Entity.FindColumn(nameof(Customer.Phone));
            var name = Entity.FindColumn(nameof(Customer.Name));
            var joinedAt = Entity.FindColumn(nameof(Customer.JoinedAt));

            var predicate = And(
                Equal(table.Column(phone), new SqlParameterExpression("Phone_0", "0912", typeof(string), phone)),
                And(
                    Equal(table.Column(name), new SqlParameterExpression("Name_0", "Johnny", typeof(string), name)),
                    Equal(table.Column(joinedAt), new SqlParameterExpression("JoinedAt_0", new DateTime(2020, 1, 2), typeof(DateTime), joinedAt))));

            var generated = new SqlServerSqlGenerator().Generate(new SelectExpression(new[] { new ProjectionExpression(table.Column(name), "Name") }, new[] { table }, predicate));

            var (_, parameters) = ApplyToCommand(generated);

            parameters.Select(p => p.ParameterName).Should().Equal("Phone_0", "Name_0", "JoinedAt_0");

            parameters[0].SqlDbType.Should().Be(SqlDbType.VarChar);
            parameters[0].Size.Should().Be(20);
            parameters[1].SqlDbType.Should().Be(SqlDbType.NVarChar);
            parameters[2].SqlDbType.Should().Be(SqlDbType.DateTime2);
        }

        [TestMethod]
        public void Dapper_replaces_literal_placeholders_with_values()
        {
            var table = TableExpression.For(Entity, "c");
            var id = Entity.FindColumn(nameof(Customer.Id));
            var name = Entity.FindColumn(nameof(Customer.Name));
            var isVip = Entity.FindColumn(nameof(Customer.IsVip));
            var level = Entity.FindColumn(nameof(Customer.Level));

            var predicate = And(
                Equal(table.Column(id), new SqlParameterExpression("Id_0", 18, typeof(int), id)),
                And(
                    Equal(table.Column(isVip), new SqlParameterExpression("IsVip_0", true, typeof(bool), isVip)),
                    And(
                        Equal(table.Column(level), new SqlParameterExpression("Level_0", CustomerLevel.Gold, typeof(CustomerLevel), level)),
                        Equal(table.Column(name), new SqlParameterExpression("Name_0", "Johnny", typeof(string), name)))));

            var generated = new SqlServerSqlGenerator().Generate(new SelectExpression(new[] { new ProjectionExpression(table.Column(name), "Name") }, new[] { table }, predicate));

            generated.Sql.Should().EndWith("WHERE ([c].[Id] = {=Id_0}) AND (([c].[IsVip] = {=IsVip_0}) AND (([c].[Level] = {=Level_0}) AND ([c].[Name] = @Name_0)))");

            var (commandText, parameters) = ApplyToCommand(generated);

            commandText.Should().EndWith("WHERE ([c].[Id] = 18) AND (([c].[IsVip] = 1) AND (([c].[Level] = 2) AND ([c].[Name] = @Name_0)))");
            parameters.Select(p => p.ParameterName).Should().Contain("Name_0");
        }

        /// <summary>
        ///     讓 Dapper 把參數加到 SqlCommand 上（不連線資料庫），回傳替換字面值之後的 SQL 與最後送出的參數。
        ///     Dapper 2.0.4 的 SqlMapper.Identity 只有 internal 建構子，所以用反射建立；它會讀取連線字串，但不會開啟連線。
        /// </summary>
        private static (string CommandText, SqlParameter[] Parameters) ApplyToCommand(GeneratedSql generated)
        {
            var parameters = SqlServerParameterMapper.ToDynamicParameters(generated);

            using (var connection = new SqlConnection())
            using (var command = new SqlCommand(generated.Sql))
            {
                var identity = (SqlMapper.Identity)Activator.CreateInstance(
                    typeof(SqlMapper.Identity),
                    BindingFlags.Instance | BindingFlags.NonPublic,
                    null,
                    new object[] { generated.Sql, CommandType.Text, connection, typeof(object), typeof(DynamicParameters) },
                    null);

                ((SqlMapper.IDynamicParameters)parameters).AddParameters(command, identity);

                return (command.CommandText, command.Parameters.Cast<SqlParameter>().ToArray());
            }
        }

        private static (object, DbType?) Map(string propertyName, object value)
        {
            var property = Entity.FindColumn(propertyName);

            return SqlServerParameterMapper.Map(new SqlParameterExpression(propertyName + "_0", value, property.ClrType, property));
        }

        private static DbString MapDbString(string propertyName, string value)
        {
            var (mapped, dbType) = Map(propertyName, value);

            dbType.Should().BeNull();

            return mapped.Should().BeOfType<DbString>().Subject;
        }

        private static SqlBinaryExpression Equal(SqlExpression left, SqlExpression right)
        {
            return new SqlBinaryExpression(SqlBinaryOperator.Equal, left, right, typeof(bool));
        }

        private static SqlBinaryExpression And(SqlExpression left, SqlExpression right)
        {
            return new SqlBinaryExpression(SqlBinaryOperator.AndAlso, left, right, typeof(bool));
        }

        private enum CustomerLevel
        {
            Normal = 0,
            Gold = 2
        }

        [Table("tbl_customer")]
        private class Customer
        {
            public int Id { get; set; }

            public string Name { get; set; }

            public bool IsVip { get; set; }

            public CustomerLevel Level { get; set; }

            [Column(TypeName = "varchar")]
            [StringLength(20)]
            public string Phone { get; set; }

            [Column(TypeName = "VarChar")]
            public string Email { get; set; }

            [Column(TypeName = "varchar(30)")]
            [StringLength(30)]
            public string Address { get; set; }

            [Column(TypeName = "char")]
            [StringLength(2)]
            public string Code { get; set; }

            [Column(TypeName = "nchar")]
            [StringLength(4)]
            public string Region { get; set; }

            [Column(TypeName = "nvarchar")]
            public string Nickname { get; set; }

            [Column(TypeName = "ntext")]
            public string Memo { get; set; }

            [Column(TypeName = "datetime2")]
            public DateTime JoinedAt { get; set; }

            [Column(TypeName = "datetime2(3)")]
            public DateTime? PreciseAt { get; set; }

            [Column(TypeName = "date")]
            public DateTime BirthDate { get; set; }

            [Column(TypeName = "time")]
            public TimeSpan OpenAt { get; set; }

            [Column(TypeName = "datetime")]
            public DateTime CreatedAt { get; set; }
        }
    }
}
