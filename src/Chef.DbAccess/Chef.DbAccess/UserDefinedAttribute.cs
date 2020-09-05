using System;

namespace Chef.DbAccess
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class UserDefinedAttribute : Attribute
    {
        public string TableType { get; set; }
    }
}