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

        public DepartmentForBenchmark Department { get; set; }

        public int ManagerId { get; set; }

        public MemberForBenchmark Manager { get; set; }

        [NotMapped]
        public string Secret { get; set; }
    }
}