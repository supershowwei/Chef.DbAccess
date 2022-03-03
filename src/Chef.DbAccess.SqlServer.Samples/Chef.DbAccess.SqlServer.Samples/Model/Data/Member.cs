using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Chef.DbAccess.SqlServer.Samples.Model.Data
{
    [ConnectionString("MemberDB")]
    [ConnectionString("AnotherMemberDB")]
    [ConnectionString("nameOrConnectionString")]
    [UserDefined(TableType = "MemberType")]
    [Table("tbl_member")]
    public class Member
    {
        [Column("No")]
        [Required]
        public int Id { get; set; }

        [StringLength(50)]
        [Required]
        public string Name { get; set; }

        [Column(TypeName = "varchar")]
        [StringLength(20)]
        public string Phone { get; set; }

        public int Age { get; set; }

        [Required]
        public int DepartmentId { get; set; }

        public Department Department { get; set; }

        [Required]
        public int ManagerId { get; set; }

        public Member Manager { get; set; }

        public List<Department> ManagedDepartments { get; set; }

        [NotMapped]
        public string Secret { get; set; }
    }

    [ConnectionString("MemberDB")]
    [Table("Member")]
    public class MemberForBenchmark
    {
        public int Id { get; set; }

        [StringLength(50)]
        public string Name { get; set; }

        [Column(TypeName = "varchar")]
        public string Phone { get; set; }

        public int Age { get; set; }

        public int DepartmentId { get; set; }

        public List<DepartmentForBenchmark> Departments { get; set; }

        public int ManagerId { get; set; }

        public MemberForBenchmark Manager { get; set; }

        [NotMapped]
        public string Secret { get; set; }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;

            if (this.Id == default) return base.Equals(obj);
            if (this.Name == default) return base.Equals(obj);

            return this.Equals((MemberForBenchmark)obj);
        }

        public override int GetHashCode()
        {
            if (this.Id == default) return base.GetHashCode();
            if (this.Name == default) return base.GetHashCode();

            return HashCode.Combine(this.Id, this.Name);
        }

        protected bool Equals(MemberForBenchmark other)
        {
            return this.Id == other.Id && this.Name == other.Name;
        }
    }
}