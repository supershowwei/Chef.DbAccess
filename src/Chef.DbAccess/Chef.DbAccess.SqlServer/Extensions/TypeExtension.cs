using System;
using System.Collections.Generic;
using System.Linq;

namespace Chef.DbAccess.SqlServer.Extensions
{
    internal static class TypeExtension
    {
        private static readonly HashSet<Type> FundamentalTypes = new HashSet<Type>
                                                                 {
                                                                     typeof(bool),
                                                                     typeof(bool?),
                                                                     typeof(byte),
                                                                     typeof(byte?),
                                                                     typeof(sbyte),
                                                                     typeof(sbyte?),
                                                                     typeof(short),
                                                                     typeof(short?),
                                                                     typeof(ushort),
                                                                     typeof(ushort?),
                                                                     typeof(int),
                                                                     typeof(int?),
                                                                     typeof(uint),
                                                                     typeof(uint?),
                                                                     typeof(long),
                                                                     typeof(long?),
                                                                     typeof(ulong),
                                                                     typeof(ulong?),
                                                                     typeof(float),
                                                                     typeof(float?),
                                                                     typeof(double),
                                                                     typeof(double?),
                                                                     typeof(decimal),
                                                                     typeof(decimal?),
                                                                     typeof(string)
                                                                 };

        public static bool IsFundamental(this Type me)
        {
            return FundamentalTypes.Any(fundamentalType => me == fundamentalType);
        }
    }
}