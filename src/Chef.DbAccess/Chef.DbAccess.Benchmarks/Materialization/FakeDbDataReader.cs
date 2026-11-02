using System;
using System.Collections;
using System.Data.Common;
using System.Data.SqlTypes;

namespace Chef.DbAccess.Benchmarks.Materialization
{
    /// <summary>
    /// 記憶體中的 DbDataReader，用來量測「讀取資料列並產生物件」的成本，排除網路與 SQL Server 的雜訊。
    /// </summary>
    /// <remarks>
    /// 每一欄以強型別陣列保存。和 SqlDataReader 一樣，GetValue 每次都會把實值型別裝箱成新的物件，
    /// 強型別方法（GetInt32、GetDateTime…）則不裝箱，所以兩種讀法的配置差異和真實情況一致。
    /// 字串是事先建好的，兩種讀法都不會配置字串。
    /// </remarks>
    internal sealed class FakeDbDataReader : DbDataReader
    {
        private readonly FakeColumn[] columns;
        private readonly int rowCount;
        private int row = -1;
        private bool closed;

        public FakeDbDataReader(FakeColumn[] columns, int rowCount)
        {
            this.columns = columns;
            this.rowCount = rowCount;
        }

        public override int FieldCount => this.columns.Length;

        public override bool HasRows => this.rowCount > 0;

        public override bool IsClosed => this.closed;

        public override int RecordsAffected => -1;

        public override int Depth => 0;

        public override object this[int ordinal] => this.GetValue(ordinal);

        public override object this[string name] => this.GetValue(this.GetOrdinal(name));

        public override bool Read()
        {
            if (this.row + 1 >= this.rowCount) return false;

            this.row++;

            return true;
        }

        public override bool NextResult()
        {
            return false;
        }

        public override void Close()
        {
            this.closed = true;
        }

        public override string GetName(int ordinal)
        {
            return this.columns[ordinal].Name;
        }

        public override int GetOrdinal(string name)
        {
            for (var i = 0; i < this.columns.Length; i++)
            {
                if (string.Equals(this.columns[i].Name, name, StringComparison.OrdinalIgnoreCase)) return i;
            }

            throw new IndexOutOfRangeException(name);
        }

        public override Type GetFieldType(int ordinal)
        {
            return this.columns[ordinal].FieldType;
        }

        public override string GetDataTypeName(int ordinal)
        {
            return this.columns[ordinal].DataTypeName;
        }

        public override bool IsDBNull(int ordinal)
        {
            return this.columns[ordinal].IsNull(this.row);
        }

        public override object GetValue(int ordinal)
        {
            return this.columns[ordinal].GetValue(this.row);
        }

        public override int GetValues(object[] values)
        {
            var count = Math.Min(values.Length, this.columns.Length);

            for (var i = 0; i < count; i++) values[i] = this.GetValue(i);

            return count;
        }

        public override bool GetBoolean(int ordinal) => this.Get<bool>(ordinal);

        public override byte GetByte(int ordinal) => this.Get<byte>(ordinal);

        public override char GetChar(int ordinal) => this.Get<char>(ordinal);

        public override DateTime GetDateTime(int ordinal) => this.Get<DateTime>(ordinal);

        public override decimal GetDecimal(int ordinal) => this.Get<decimal>(ordinal);

        public override double GetDouble(int ordinal) => this.Get<double>(ordinal);

        public override float GetFloat(int ordinal) => this.Get<float>(ordinal);

        public override Guid GetGuid(int ordinal) => this.Get<Guid>(ordinal);

        public override short GetInt16(int ordinal) => this.Get<short>(ordinal);

        public override int GetInt32(int ordinal) => this.Get<int>(ordinal);

        public override long GetInt64(int ordinal) => this.Get<long>(ordinal);

        public override string GetString(int ordinal) => this.Get<string>(ordinal);

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotSupportedException();
        }

        // 和 SqlDataReader 一樣，對 NULL 呼叫強型別方法會丟出 SqlNullValueException
        private T Get<T>(int ordinal)
        {
            var column = (FakeColumn<T>)this.columns[ordinal];

            if (column.IsNull(this.row)) throw new SqlNullValueException();

            return column.Values[this.row];
        }
    }

    internal abstract class FakeColumn
    {
        protected FakeColumn(string name, string dataTypeName)
        {
            this.Name = name;
            this.DataTypeName = dataTypeName;
        }

        public string Name { get; }

        public string DataTypeName { get; }

        public abstract Type FieldType { get; }

        public abstract bool IsNull(int row);

        public abstract object GetValue(int row);
    }

    internal sealed class FakeColumn<T> : FakeColumn
    {
        private readonly bool[] nulls;

        public FakeColumn(string name, string dataTypeName, T[] values, bool[] nulls = null)
            : base(name, dataTypeName)
        {
            this.Values = values;
            this.nulls = nulls;
        }

        public T[] Values { get; }

        public override Type FieldType => typeof(T);

        public override bool IsNull(int row)
        {
            return this.nulls != null && this.nulls[row];
        }

        public override object GetValue(int row)
        {
            return this.IsNull(row) ? DBNull.Value : (object)this.Values[row];
        }
    }
}
