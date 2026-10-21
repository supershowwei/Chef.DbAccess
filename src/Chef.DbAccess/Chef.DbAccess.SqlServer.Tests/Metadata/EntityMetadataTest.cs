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
    }
}
