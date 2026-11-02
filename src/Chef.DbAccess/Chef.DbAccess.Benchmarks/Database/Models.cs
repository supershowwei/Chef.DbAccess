using System;
using System.ComponentModel.DataAnnotations;

namespace Chef.DbAccess.Benchmarks.Database
{
    [ConnectionString(BenchmarkDatabase.Name)]
    public class Member
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public string Email { get; set; }

        public int? Age { get; set; }

        public decimal Balance { get; set; }

        public bool IsActive { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTime? UpdatedAt { get; set; }

        public Guid Token { get; set; }

        public int DepartmentId { get; set; }

        public Department Department { get; set; }
    }

    /// <summary>
    /// 寫入情境用的 MemberWrite 資料表。3.x 的 Insert／BulkInsert 只寫入標了 [Required] 的屬性，
    /// 所以 Id 以外都標上；這裡的 [Required] 不代表欄位不可為 NULL。
    /// </summary>
    [ConnectionString(BenchmarkDatabase.Name)]
    public class MemberWrite
    {
        public int Id { get; set; }

        [Required]
        public string Name { get; set; }

        [Required]
        public string Email { get; set; }

        [Required]
        public int? Age { get; set; }

        [Required]
        public decimal Balance { get; set; }

        [Required]
        public bool IsActive { get; set; }

        [Required]
        public DateTime CreatedAt { get; set; }

        [Required]
        public DateTime? UpdatedAt { get; set; }

        [Required]
        public Guid Token { get; set; }

        [Required]
        public int DepartmentId { get; set; }
    }

    [ConnectionString(BenchmarkDatabase.Name)]
    public class Department
    {
        public int Id { get; set; }

        public int CompanyId { get; set; }

        public string Name { get; set; }

        public Company Company { get; set; }
    }

    [ConnectionString(BenchmarkDatabase.Name)]
    public class Company
    {
        public int Id { get; set; }

        public string Name { get; set; }
    }
}
