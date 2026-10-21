using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    [Table("tbl_member")]
    internal class Member
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

        public bool IsActive { get; set; }

        public DateTime? Birthday { get; set; }

        public Guid? Token { get; set; }

        [Required]
        public int DepartmentId { get; set; }

        public Department Department { get; set; }

        public int ManagerId { get; set; }

        public Member Manager { get; set; }

        public List<Department> ManagedDepartments { get; set; }

        [NotMapped]
        public string Secret { get; set; }
    }

    [Table("tbl_department")]
    internal class Department
    {
        [Required]
        public int Id { get; set; }

        [Required]
        public string Name { get; set; }

        public int ManagerId { get; set; }
    }

    [Table("tbl_member")]
    internal class MemberStatistics
    {
        [Column("No")]
        public int Id { get; set; }

        public string Name { get; set; }

        public int Age { get; set; }

        public decimal AverageAge { get; set; }
    }
}
