using System.Data;
using System.Reflection;

namespace Chef.DbAccess.SqlServer
{
    internal class UserDefinedField
    {
        public UserDefinedField(PropertyInfo property, DataColumn column)
        {
            this.Property = property;
            this.Column = column;
        }

        public PropertyInfo Property { get; }

        public DataColumn Column { get; }
    }
}