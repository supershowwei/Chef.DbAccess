﻿using System.Text;

namespace Chef.DbAccess.SqlServer.Extensions
{
    internal static class StringBuilderExtension
    {
        public static void AliasAppend(this StringBuilder me, string value, string alias)
        {
            if (!string.IsNullOrEmpty(alias)) me.Append($"{alias}.");

            me.Append(value);
        }
    }
}