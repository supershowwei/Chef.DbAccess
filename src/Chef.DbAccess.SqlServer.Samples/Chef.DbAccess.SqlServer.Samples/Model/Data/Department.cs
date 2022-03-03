using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Chef.DbAccess.SqlServer.Samples.Model.Data
{
    [ConnectionString("nameOrConnectionString")]
    [Table("tbl_department")]
    public class Department
    {
        [Required]
        public int Id { get; set; }

        [Required]
        public string Name { get; set; }

        public int ManagerId { get; set; }
    }

    [ConnectionString("MemberDB")]
    [Table("Department")]
    public class DepartmentForBenchmark
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;

            if (this.Id == default) return base.Equals(obj);

            return this.Equals((DepartmentForBenchmark)obj);
        }

        public override int GetHashCode()
        {
            if (this.Id == default) return base.GetHashCode();

            return this.Id;
        }

        protected bool Equals(DepartmentForBenchmark other)
        {
            return this.Id == other.Id;
        }
    }
}