using System;

namespace Chef.DbAccess.SqlServer.Extensions
{
    internal static class GuidExtension
    {
        public static string Purify(this Guid me)
        {
            return me.ToString().Replace("-", string.Empty);
        }
    }
}