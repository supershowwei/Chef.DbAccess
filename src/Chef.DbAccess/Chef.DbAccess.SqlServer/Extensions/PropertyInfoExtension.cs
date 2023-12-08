using System.Reflection;

namespace Chef.DbAccess.SqlServer.Extensions
{
    internal static class PropertyInfoExtension
    {
        public static string GetFullName(this PropertyInfo me)
        {
            return $"{me.ReflectedType.FullName}.{me.Name}";
        }
    }
}