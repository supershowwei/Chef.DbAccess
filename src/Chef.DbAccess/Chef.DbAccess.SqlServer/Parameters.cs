using System;
using System.Collections.Generic;

namespace Chef.DbAccess.SqlServer
{
    internal class Parameters
    {
        private static readonly Lazy<Parameters> Lazy = new Lazy<Parameters>(() => new Parameters());

        private Parameters()
        {
            this.Empty = new Dictionary<string, object>();
        }

        public static Parameters Instance => Lazy.Value;

        public Dictionary<string, object> Empty { get; }
    }
}