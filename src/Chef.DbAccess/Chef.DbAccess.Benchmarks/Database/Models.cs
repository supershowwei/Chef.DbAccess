using System;

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
