using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Chef.DbAccess.SqlServer.Samples.Model.Data;

namespace Chef.DbAccess.SqlServer.Samples
{
    public class QueryBenchmark
    {
        private readonly IDataAccessFactory dataAccessFactory;

        public QueryBenchmark()
        {
            this.dataAccessFactory = SqlServerDataAccessFactory.Instance;
        }

        public void InnerJoin()
        {
            var memberDataAccess = this.dataAccessFactory.Create<MemberForBenchmark>();

            var members = memberDataAccess.InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id)
                .InnerJoin((x, y) => x.Manager, (x, y, z) => x.ManagerId == z.Id)
                .Where((x, y, z) => x.Age > 20)
                .Select(
                    (x, y, z) => new
                    {
                        x.Id,
                        x.Name,
                        DepartmentId = y.Id,
                        DepartmentName = y.Name,
                        ManagerId = z.Id,
                        ManagerName = z.Name
                    })
                .QueryAsync()
                .Result;
        }
    }
}