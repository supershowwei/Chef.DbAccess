using System;

namespace Chef.DbAccess
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class ConnectionStringAttribute : Attribute
    {
        public ConnectionStringAttribute(string nameOrConnectionString)
        {
            this.ConnectionString = nameOrConnectionString;
            this.Schema = "dbo";
        }

        public string ConnectionString { get; }

        /// <summary>
        ///     Default is 'dbo'.
        /// </summary>
        public string Schema { get; set; }
    }
}