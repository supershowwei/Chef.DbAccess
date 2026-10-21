using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Chef.DbAccess.Metadata;
using Chef.DbAccess.SqlExpressions;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.SqlExpressions
{
    [TestClass]
    public class SqlExpressionTest
    {
        private static readonly EntityMetadata MemberEntity = EntityMetadata.For<Member>();

        [TestMethod]
        public void Table_and_columns_are_created_from_metadata()
        {
            var table = TableExpression.For(MemberEntity, "mem_1");
            var id = table.Column(MemberEntity.FindColumn(nameof(Member.Id)));

            table.Name.Should().Be("tbl_member");
            table.Schema.Should().Be("dbo");
            table.Alias.Should().Be("mem_1");
            table.Entity.Should().BeSameAs(MemberEntity);

            id.Table.Should().BeSameAs(table);
            id.Name.Should().Be("No");
            id.Type.Should().Be(typeof(int));
            id.Property.Should().BeSameAs(MemberEntity.FindColumn(nameof(Member.Id)));
        }

        [TestMethod]
        public void Select_describes_where_order_by_and_take()
        {
            // 對應 3.x：Where(x => x.Age >= 30 && x.Name.StartsWith("J")).OrderBy(x => x.Age).Take(10).Select(x => new { x.Name, x.Age })
            var table = TableExpression.For(MemberEntity, "mem_1");
            var name = table.Column(MemberEntity.FindColumn(nameof(Member.Name)));
            var age = table.Column(MemberEntity.FindColumn(nameof(Member.Age)));

            var predicate = new SqlBinaryExpression(
                SqlBinaryOperator.AndAlso,
                new SqlBinaryExpression(SqlBinaryOperator.GreaterThanOrEqual, age, new SqlParameterExpression("Age_0", 30, typeof(int)), typeof(bool)),
                new SqlBinaryExpression(
                    SqlBinaryOperator.Like,
                    name,
                    new SqlBinaryExpression(SqlBinaryOperator.Add, new SqlParameterExpression("Name_0", "J", typeof(string)), new SqlConstantExpression("%", typeof(string)), typeof(string)),
                    typeof(bool)),
                typeof(bool));

            var select = new SelectExpression(
                new[] { new ProjectionExpression(name, nameof(Member.Name)), new ProjectionExpression(age, nameof(Member.Age)) },
                new[] { table },
                predicate,
                new[] { new OrderingExpression(age, true) },
                limit: new SqlConstantExpression(10, typeof(int)));

            select.Projections.Should().HaveCount(2);
            select.Projections[0].Expression.Should().BeSameAs(name);
            select.Tables.Should().ContainSingle().Which.Should().BeSameAs(table);
            select.Predicate.Should().BeSameAs(predicate);
            select.Orderings.Should().ContainSingle().Which.IsAscending.Should().BeTrue();
            select.Offset.Should().BeNull();
            ((SqlConstantExpression)select.Limit).Value.Should().Be(10);
            select.IsDistinct.Should().BeFalse();
        }

        [TestMethod]
        public void Select_defaults_to_no_predicate_ordering_or_paging()
        {
            var table = TableExpression.For(MemberEntity, "mem_1");

            var select = new SelectExpression(new[] { new ProjectionExpression(table.Column(MemberEntity.FindColumn(nameof(Member.Id))), "Id") }, new[] { table });

            select.Predicate.Should().BeNull();
            select.Orderings.Should().BeEmpty();
            select.Offset.Should().BeNull();
            select.Limit.Should().BeNull();
        }

        [TestMethod]
        public void Collections_are_copied_so_later_changes_do_not_leak_in()
        {
            var table = TableExpression.For(MemberEntity, "mem_1");
            var id = table.Column(MemberEntity.FindColumn(nameof(Member.Id)));
            var projections = new List<ProjectionExpression> { new ProjectionExpression(id, "Id") };
            var values = new List<SqlExpression> { new SqlParameterExpression("Id_0", 1, typeof(int)) };

            var select = new SelectExpression(projections, new[] { table });
            var @in = new InExpression(id, values);

            projections.Add(new ProjectionExpression(id, "Id2"));
            values.Clear();

            select.Projections.Should().HaveCount(1);
            @in.Values.Should().HaveCount(1);
        }

        [TestMethod]
        public void In_expression_is_boolean_and_allows_empty_values()
        {
            var table = TableExpression.For(MemberEntity, "mem_1");

            var @in = new InExpression(table.Column(MemberEntity.FindColumn(nameof(Member.Id))), Array.Empty<SqlExpression>(), isNegated: true);

            @in.Type.Should().Be(typeof(bool));
            @in.Values.Should().BeEmpty();
            @in.IsNegated.Should().BeTrue();
        }

        [TestMethod]
        public void Empty_schema_is_normalized_to_null()
        {
            new TableExpression("tbl_member", string.Empty, "m").Schema.Should().BeNull();
        }

        [TestMethod]
        public void Select_requires_projections_and_tables()
        {
            var table = TableExpression.For(MemberEntity, "mem_1");
            var projection = new ProjectionExpression(table.Column(MemberEntity.FindColumn(nameof(Member.Id))), "Id");

            Action noProjection = () => new SelectExpression(Array.Empty<ProjectionExpression>(), new[] { table });
            Action noTable = () => new SelectExpression(new[] { projection }, Array.Empty<TableExpressionBase>());
            Action nullOrdering = () => new SelectExpression(new[] { projection }, new[] { table }, orderings: new OrderingExpression[] { null });

            noProjection.Should().Throw<ArgumentException>();
            noTable.Should().Throw<ArgumentException>();
            nullOrdering.Should().Throw<ArgumentException>();
        }

        [TestMethod]
        public void Nodes_reject_missing_required_parts()
        {
            var table = new TableExpression("tbl_member", null, "m");
            var column = new ColumnExpression(table, "No", typeof(int));

            new Action(() => new ColumnExpression(table, "", typeof(int))).Should().Throw<ArgumentException>();
            new Action(() => new ColumnExpression(null, "No", typeof(int))).Should().Throw<ArgumentNullException>();
            new Action(() => new SqlParameterExpression("", 1, typeof(int))).Should().Throw<ArgumentException>();
            new Action(() => new SqlConstantExpression(1, null)).Should().Throw<ArgumentNullException>();
            new Action(() => new SqlBinaryExpression(SqlBinaryOperator.Equal, column, null, typeof(bool))).Should().Throw<ArgumentNullException>();
            new Action(() => new SqlUnaryExpression(SqlUnaryOperator.IsNull, null, typeof(bool))).Should().Throw<ArgumentNullException>();
            new Action(() => new SqlFunctionExpression("LEN", new SqlExpression[] { null }, typeof(int))).Should().Throw<ArgumentException>();
            new Action(() => new TableExpression("tbl_member", null, "")).Should().Throw<ArgumentException>();
            new Action(() => new ProjectionExpression(column, "")).Should().Throw<ArgumentException>();
        }

        [TestMethod]
        public void Join_uses_alias_of_joined_table()
        {
            var member = TableExpression.For(MemberEntity, "mem_1");
            var manager = TableExpression.For(MemberEntity, "mem_2");
            var on = new SqlBinaryExpression(SqlBinaryOperator.Equal, member.Column(MemberEntity.FindColumn(nameof(Member.Id))), manager.Column(MemberEntity.FindColumn(nameof(Member.Id))), typeof(bool));

            var join = new JoinExpression(JoinType.Left, manager, on);

            join.Alias.Should().Be("mem_2");
            join.Table.Should().BeSameAs(manager);
            join.JoinType.Should().Be(JoinType.Left);
            join.Predicate.Should().BeSameAs(on);
        }

        [TestMethod]
        public void Join_rejects_missing_parts_and_nested_join()
        {
            var member = TableExpression.For(MemberEntity, "mem_1");
            var manager = TableExpression.For(MemberEntity, "mem_2");
            var on = new SqlConstantExpression(true, typeof(bool));
            var join = new JoinExpression(JoinType.Inner, manager, on);

            new Action(() => new JoinExpression(JoinType.Inner, null, on)).Should().Throw<ArgumentNullException>();
            new Action(() => new JoinExpression(JoinType.Inner, manager, null)).Should().Throw<ArgumentNullException>();
            new Action(() => new JoinExpression(JoinType.Inner, join, on)).Should().Throw<ArgumentException>();

            var projection = new ProjectionExpression(member.Column(MemberEntity.FindColumn(nameof(Member.Id))), "Id");

            new Action(() => new SelectExpression(new[] { projection }, new TableExpressionBase[] { join, member })).Should().Throw<ArgumentException>();
        }

        [Table("tbl_member", Schema = "dbo")]
        private class Member
        {
            [Column("No")]
            [Required]
            public int Id { get; set; }

            public string Name { get; set; }

            public int Age { get; set; }
        }
    }
}
