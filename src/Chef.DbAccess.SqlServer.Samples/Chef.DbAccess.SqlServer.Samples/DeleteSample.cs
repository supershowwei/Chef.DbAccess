using System.Threading.Tasks;
using Chef.DbAccess.Fluent;
using Chef.DbAccess.SqlServer.Samples.Model.Data;

namespace Chef.DbAccess.SqlServer.Samples
{
    public class DeleteSample
    {
        private static async Task DemoDelete()
        {
            IDataAccessFactory dataAccessFactory = SqlServerDataAccessFactory.Instance;

            var memberDataAccess = dataAccessFactory.Create<Member>();

            // DELETE Name LIKE '%A%' 的 Member
            await memberDataAccess
                .Where(x => x.Name.Contains("A"))
                .DeleteAsync();

            // 刪除 Name 有 'A' 且 Department Name 有 '業務' 字樣的 Member
            await memberDataAccess
                .InnerJoin(x => x.Department, (x, y) => x.DepartmentId == y.Id)
                .Where((x, y) => x.Name.Contains("A") && y.Name.Contains("業務"))
                .DeleteAsync();
        }
    }
}