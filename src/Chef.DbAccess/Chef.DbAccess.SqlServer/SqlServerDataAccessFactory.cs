using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Chef.DbAccess.SqlServer
{
    public class SqlServerDataAccessFactory : IDataAccessFactory
    {
        private static readonly Lazy<SqlServerDataAccessFactory> Lazy = new Lazy<SqlServerDataAccessFactory>(() => new SqlServerDataAccessFactory());

        private static readonly ConcurrentDictionary<string, string> ConnectionStrings = new ConcurrentDictionary<string, string>();

        private SqlServerDataAccessFactory()
        {
        }

        public static SqlServerDataAccessFactory Instance => Lazy.Value;

        public Action<Exception, string, object> OnDbError { get; set; }

        public IDataAccess<T> Create<T>()
        {
            return this.Create<T>(null, null);
        }

        public IDataAccess<T> Create<T>(string tableName)
        {
            return this.Create<T>(tableName, null);
        }

        public IDataAccess<T> Create<T>(string tableName, string nameOrConnectionString)
        {
            var connectionStringAttributes = typeof(T).GetCustomAttributes<ConnectionStringAttribute>(true);

            ConnectionStringAttribute connectionStringAttribute;

            if (string.IsNullOrEmpty(nameOrConnectionString))
            {
                if (!connectionStringAttributes.Any())
                {
                    throw new ArgumentException("Must add connection string.");
                }

                if (connectionStringAttributes.Count() > 1)
                {
                    throw new ArgumentException("Must indicate connection string.");
                }

                connectionStringAttribute = connectionStringAttributes.Single();
            }
            else
            {
                connectionStringAttribute = connectionStringAttributes.SingleOrDefault(x => x.ConnectionString == nameOrConnectionString);
            }

            string connectionString;

            if (connectionStringAttribute != null)
            {
                connectionString = ConnectionStrings.ContainsKey(connectionStringAttribute.ConnectionString)
                                           ? ConnectionStrings[connectionStringAttribute.ConnectionString]
                                           : connectionStringAttribute.ConnectionString;
            }
            else
            {
                connectionString = nameOrConnectionString;
            }

            return new SqlServerDataAccess<T>(tableName, connectionString) { OnDbError = this.OnDbError };
        }

        public void AddConnectionString(string name, string value)
        {
            ConnectionStrings.TryAdd(name, value);
        }

        internal string GetConnectionString(string nameOrConnectionString)
        {
            return ConnectionStrings.ContainsKey(nameOrConnectionString)
                       ? ConnectionStrings[nameOrConnectionString]
                       : nameOrConnectionString;
        }
    }
}