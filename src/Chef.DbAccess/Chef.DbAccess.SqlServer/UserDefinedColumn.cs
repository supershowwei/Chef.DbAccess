using System.Data;
using System.Reflection;
using Microsoft.Data.SqlClient.Server;

namespace Chef.DbAccess.SqlServer
{
    internal class UserDefinedField
    {
        public UserDefinedField(PropertyInfo property, SqlMetaData column)
        {
            this.Property = property;
            this.Column = column;
        }

        public PropertyInfo Property { get; }

        public SqlMetaData Column { get; }
    }
}