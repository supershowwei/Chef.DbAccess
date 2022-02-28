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
    }

    [ConnectionString("MemberDB")]
    [Table("Department")]
    public class DepartmentForBenchmark
    {
        [Required]
        public int Id { get; set; }

        [Required]
        public string Name { get; set; }
    }
}