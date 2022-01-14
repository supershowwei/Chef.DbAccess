using System.ComponentModel.DataAnnotations.Schema;

namespace Chef.DbAccess.SqlServer.Samples.Model.Data
{
    [ConnectionString("nameOrConnectionString")]
    [Table("tbl_member")]
    public class MemberStatistics
    {
        [Column("No")]
        public int Id { get; set; }

        public string Name { get; set; }

        public int Age { get; set; }

        public decimal AverageAge { get; set; }
    }
}