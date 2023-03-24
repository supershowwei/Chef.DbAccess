using System;
using System.Collections.Generic;

namespace Chef.DbAccess
{
    public interface IDataAccessFactory
    {
        Action<Exception, string, object> OnDbError { get; set; }

        IDataAccess<T> Create<T>();

        IDataAccess<T> Create<T>(string tableName);

        IDataAccess<T> Create<T>(string tableName, string nameOrConnectionString);

        void AddConnectionString(string name, string value);
    }
}