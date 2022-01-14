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
        }
    }
}