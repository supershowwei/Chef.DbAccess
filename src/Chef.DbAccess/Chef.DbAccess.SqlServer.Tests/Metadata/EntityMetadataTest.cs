using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Chef.DbAccess.Metadata;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chef.DbAccess.SqlServer.Tests.Metadata
{
    [TestClass]
    public class EntityMetadataTest
    {
        [TestMethod]
        public void Table_name_and_schema_come_from_TableAttribute()
        {
            var metadata = EntityMetadata.For<Employee>();

            metadata.TableName.Should().Be("tbl_employee");
            metadata.Schema.Should().Be("hr");
        }

        [TestMethod]
        public void Table_name_falls_back_to_class_name()
        {
            var metadata = EntityMetadata.For<Team>();

            metadata.TableName.Should().Be(nameof(Team));
            metadata.Schema.Should().BeNull();
        }

        [TestMethod]
        public void Column_name_comes_from_ColumnAttribute_or_property_name()
        {
            var metadata = EntityMetadata.For<Employee>();

            metadata.FindColumn(nameof(Employee.Id)).ColumnName.Should().Be("No");
            metadata.FindColumn(nameof(Employee.Name)).ColumnName.Should().Be(nameof(Employee.Name));
        }

        [TestMethod]
        public void Columns_include_scalar_nullable_enum_and_binary_properties()
        {
            var metadata = EntityMetadata.For<Employee>();

            metadata.Columns.Select(c => c.PropertyName)
                .Should()
                .BeEquivalentTo(
                    nameof(Employee.Id),
                    nameof(Employee.Name),
                    nameof(Employee.Phone),
                    nameof(Employee.Level),
                    nameof(Employee.Status),
                    nameof(Employee.HiredAt),
                    nameof(Employee.Photo),
                    nameof(Employee.TeamId),
                    nameof(Employee.Version));
        }

        [TestMethod]
        public void Columns_exclude_NotMapped_navigation_and_indexer_properties()
        {
            var metadata = EntityMetadata.For<Employee>();

            metadata.FindColumn(nameof(Employee.Secret)).Should().BeNull();
            metadata.FindColumn(nameof(Employee.Team)).Should().BeNull();
            metadata.FindColumn(nameof(Employee.Projects)).Should().BeNull();
            metadata.FindColumn("Item").Should().BeNull();
        }

        [TestMethod]
        public void Column_type_name_and_max_length_are_read()
        {
            var metadata = EntityMetadata.For<Employee>();

            var phone = metadata.FindColumn(nameof(Employee.Phone));
            phone.ColumnTypeName.Should().Be("varchar");
            phone.MaxLength.Should().Be(20);

            var name = metadata.FindColumn(nameof(Employee.Name));
            name.ColumnTypeName.Should().BeNull();
            name.MaxLength.Should().Be(50);

            metadata.FindColumn(nameof(Employee.Photo)).MaxLength.Should().Be(1024);
            metadata.FindColumn(nameof(Employee.Level)).MaxLength.Should().BeNull();
        }

        [TestMethod]
        public void Key_required_identity_and_computed_flags_are_read()
        {
            var metadata = EntityMetadata.For<Employee>();

            var id = metadata.FindColumn(nameof(Employee.Id));
            id.IsKey.Should().BeTrue();
            id.IsRequired.Should().BeTrue();
            id.IsIdentity.Should().BeTrue();
            id.IsComputed.Should().BeFalse();

            var version = metadata.FindColumn(nameof(Employee.Version));
            version.IsComputed.Should().BeTrue();
            version.IsIdentity.Should().BeFalse();

            metadata.FindColumn(nameof(Employee.Phone)).IsRequired.Should().BeFalse();

            metadata.Keys.Should().ContainSingle().Which.Should().BeSameAs(id);
        }

        [TestMethod]
        public void Inherited_properties_are_columns_and_derived_table_attribute_wins()
        {
            var metadata = EntityMetadata.For<Manager>();

            metadata.TableName.Should().Be("tbl_manager");
            metadata.FindColumn(nameof(Manager.Id)).ColumnName.Should().Be("No");
            metadata.FindColumn(nameof(Manager.Bonus)).Should().NotBeNull();
        }

        [TestMethod]
        public void Hidden_property_resolves_to_derived_declaration()
        {
            var metadata = EntityMetadata.For<RenamedTeam>();

            var column = metadata.FindColumn(nameof(RenamedTeam.Name));

            column.Property.DeclaringType.Should().Be(typeof(RenamedTeam));
            column.ColumnName.Should().Be("team_name");
        }

        [TestMethod]
        public void Metadata_is_cached_per_type()
        {
            EntityMetadata.For<Employee>().Should().BeSameAs(EntityMetadata.For(typeof(Employee)));
            EntityMetadata.For<Employee>().Should().NotBeSameAs(EntityMetadata.For<Team>());
        }

        [TestMethod]
        public void For_null_type_throws()
        {
            Action act = () => EntityMetadata.For(null);

            act.Should().Throw<ArgumentNullException>();
        }

        [TestMethod]
        public void Navigations_come_from_ForeignKey_on_navigation_or_foreign_key()
        {
            var metadata = EntityMetadata.For<Order>();

            // 沒有 [ForeignKey] 的 Referrer、集合 Others 都不是導覽屬性。
            metadata.Navigations.Select(n => n.PropertyName).Should().BeEquivalentTo(nameof(Order.Customer), nameof(Order.Product), nameof(Order.Region), nameof(Order.Parent));
            metadata.FindNavigation(nameof(Order.Referrer)).Should().BeNull();
            metadata.FindColumn(nameof(Order.Customer)).Should().BeNull();

            var customer = metadata.FindNavigation(nameof(Order.Customer));
            customer.TargetType.Should().Be(typeof(Customer));
            customer.ForeignKeys.Select(k => k.ColumnName).Should().Equal("customer_id");

            metadata.FindNavigation(nameof(Order.Product)).ForeignKeys.Select(k => k.PropertyName).Should().Equal(nameof(Order.ProductCode));
        }

        [TestMethod]
        public void Principal_keys_are_Key_columns_or_Id()
        {
            var metadata = EntityMetadata.For<Order>();

            metadata.FindNavigation(nameof(Order.Customer)).PrincipalKeys.Select(k => k.PropertyName).Should().Equal(nameof(Customer.Id));
            metadata.FindNavigation(nameof(Order.Product)).PrincipalKeys.Select(k => k.PropertyName).Should().Equal(nameof(Product.Code));

            var region = metadata.FindNavigation(nameof(Order.Region));
            region.ForeignKeys.Select(k => k.PropertyName).Should().Equal(nameof(Order.RegionCountry), nameof(Order.RegionCode));
            region.PrincipalKeys.Select(k => k.PropertyName).Should().Equal(nameof(Region.Country), nameof(Region.Code));
        }

        [TestMethod]
        public void Self_reference_does_not_recurse()
        {
            var parent = EntityMetadata.For<Order>().FindNavigation(nameof(Order.Parent));

            parent.TargetEntity.Should().BeSameAs(EntityMetadata.For<Order>());
            parent.PrincipalKeys.Select(k => k.PropertyName).Should().Equal(nameof(Order.Id));
        }

        [TestMethod]
        public void Invalid_ForeignKey_throws()
        {
            Action missingForeignKey = () => EntityMetadata.For<OrderWithMissingForeignKey>();
            Action missingNavigation = () => EntityMetadata.For<OrderWithMissingNavigation>();
            Action keyCountMismatch = () => _ = EntityMetadata.For<OrderWithKeyCountMismatch>().FindNavigation(nameof(OrderWithKeyCountMismatch.Region)).PrincipalKeys;

            missingForeignKey.Should().Throw<InvalidOperationException>().WithMessage("*refers to 'Missing'*");
            missingNavigation.Should().Throw<InvalidOperationException>().WithMessage("*[ForeignKey(\"Nope\")]*");
            keyCountMismatch.Should().Throw<InvalidOperationException>().WithMessage("*has 1 foreign key(s), but 'Region' has 2 key(s)*");
        }

        private enum EmployeeStatus
        {
            Active,
            Resigned
        }

        [Table("tbl_employee", Schema = "hr")]
        private class Employee
        {
            [Key]
            [Required]
            [Column("No")]
            [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
            public int Id { get; set; }

            [Required]
            [StringLength(50)]
            public string Name { get; set; }

            [Column(TypeName = "varchar")]
            [StringLength(20)]
            public string Phone { get; set; }

            public int? Level { get; set; }

            public EmployeeStatus? Status { get; set; }

            public DateTimeOffset HiredAt { get; set; }

            [MaxLength(1024)]
            public byte[] Photo { get; set; }

            public int TeamId { get; set; }

            public Team Team { get; set; }

            public List<Team> Projects { get; set; }

            [DatabaseGenerated(DatabaseGeneratedOption.Computed)]
            public long Version { get; set; }

            [NotMapped]
            public string Secret { get; set; }

            public string this[int index] => null;
        }

        [Table("tbl_manager")]
        private class Manager : Employee
        {
            public decimal Bonus { get; set; }
        }

        private class Team
        {
            public int Id { get; set; }

            public string Name { get; set; }
        }

        private class RenamedTeam : Team
        {
            [Column("team_name")]
            public new string Name { get; set; }
        }

        private class Order
        {
            public int Id { get; set; }

            [Column("customer_id")]
            public int CustomerId { get; set; }

            [ForeignKey(nameof(CustomerId))]
            public Customer Customer { get; set; }

            [ForeignKey(nameof(Product))]
            public int ProductCode { get; set; }

            public Product Product { get; set; }

            public string RegionCountry { get; set; }

            public string RegionCode { get; set; }

            [ForeignKey("RegionCountry, RegionCode")]
            public Region Region { get; set; }

            public int? ParentId { get; set; }

            [ForeignKey(nameof(ParentId))]
            public Order Parent { get; set; }

            public Customer Referrer { get; set; }

            public List<Customer> Others { get; set; }
        }

        private class Customer
        {
            public int Id { get; set; }
        }

        private class Product
        {
            [Key]
            public int Code { get; set; }
        }

        private class Region
        {
            [Key]
            public string Country { get; set; }

            [Key]
            public string Code { get; set; }
        }

        private class OrderWithMissingForeignKey
        {
            [ForeignKey("Missing")]
            public Customer Customer { get; set; }
        }

        private class OrderWithMissingNavigation
        {
            [ForeignKey("Nope")]
            public int CustomerId { get; set; }
        }

        private class OrderWithKeyCountMismatch
        {
            public string RegionCode { get; set; }

            [ForeignKey(nameof(RegionCode))]
            public Region Region { get; set; }
        }
    }
}
