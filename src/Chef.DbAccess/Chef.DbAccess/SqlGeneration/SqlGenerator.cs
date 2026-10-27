using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Chef.DbAccess.SqlExpressions;

namespace Chef.DbAccess.SqlGeneration
{
    /// <summary>
    ///     把 SQL AST 輸出成 SQL 字串。預設輸出 ANSI 風格的 SQL，各資料庫方言覆寫差異的部分
    ///     （識別字跳脫、參數格式、常數、運算子、分頁語法……）。
    ///     <para>
    ///         產生過程會使用執行個體上的狀態，所以不是執行緒安全的；同一個執行個體可以重複使用，
    ///         但不能同時用在多個執行緒。
    ///     </para>
    /// </summary>
    public class SqlGenerator
    {
        private readonly StringBuilder sql = new StringBuilder();
        private readonly List<SqlParameterExpression> parameters = new List<SqlParameterExpression>();

        protected StringBuilder Sql => this.sql;

        public GeneratedSql Generate(SelectExpression select)
        {
            if (select == null) throw new ArgumentNullException(nameof(select));

            this.sql.Clear();
            this.parameters.Clear();

            this.VisitSelect(select);

            return new GeneratedSql(this.sql.ToString(), this.parameters.ToArray());
        }

        protected virtual void VisitSelect(SelectExpression select)
        {
            this.sql.Append("SELECT ");

            if (select.IsDistinct) this.sql.Append("DISTINCT ");

            this.GenerateTop(select);
            this.GenerateList(select.Projections, this.VisitProjection);

            this.sql.Append("\nFROM ");
            this.GenerateTables(select.Tables);

            if (select.Predicate != null)
            {
                this.sql.Append("\nWHERE ");
                this.Visit(select.Predicate);
            }

            if (select.Orderings.Count > 0)
            {
                this.sql.Append("\nORDER BY ");
                this.GenerateList(select.Orderings, this.VisitOrdering);
            }

            this.GenerateLimitOffset(select);
        }

        /// <summary>
        ///     在 <c>SELECT [DISTINCT]</c> 之後、投影清單之前輸出內容，例如 SQL Server 的 <c>TOP(n)</c>。ANSI 不輸出任何內容。
        /// </summary>
        protected virtual void GenerateTop(SelectExpression select)
        {
        }

        /// <summary>
        ///     ANSI（SQL:2008）的 <c>OFFSET n ROWS FETCH NEXT m ROWS ONLY</c>。
        /// </summary>
        protected virtual void GenerateLimitOffset(SelectExpression select)
        {
            if (select.Offset != null)
            {
                this.sql.Append("\nOFFSET ");
                this.Visit(select.Offset);
                this.sql.Append(" ROWS");

                if (select.Limit != null)
                {
                    this.sql.Append("\nFETCH NEXT ");
                    this.Visit(select.Limit);
                    this.sql.Append(" ROWS ONLY");
                }
            }
            else if (select.Limit != null)
            {
                this.sql.Append("\nFETCH FIRST ");
                this.Visit(select.Limit);
                this.sql.Append(" ROWS ONLY");
            }
        }

        /// <summary>
        ///     欄位名稱與別名相同時省略 <c>AS</c>，例如 <c>[m].[Name]</c>；不同時為 <c>[m].[No] AS [Id]</c>。
        /// </summary>
        protected virtual void VisitProjection(ProjectionExpression projection)
        {
            this.Visit(projection.Expression);

            if (projection.Expression is ColumnExpression column && column.Name == projection.Alias) return;

            this.sql.Append(" AS ").Append(this.Delimit(projection.Alias));
        }

        /// <summary>
        ///     Join 各自換行輸出；其他來源之間以逗號分隔。
        /// </summary>
        protected virtual void GenerateTables(IReadOnlyList<TableExpressionBase> tables)
        {
            for (var i = 0; i < tables.Count; i++)
            {
                if (i > 0) this.sql.Append(tables[i] is JoinExpression || tables[i] is CrossJoinExpression ? "\n" : ", ");

                this.VisitTable(tables[i]);
            }
        }

        protected virtual void VisitTable(TableExpressionBase table)
        {
            switch (table)
            {
                case TableExpression tableExpr:
                    this.VisitTableExpression(tableExpr);
                    break;

                case JoinExpression join:
                    this.VisitJoin(join);
                    break;

                case CrossJoinExpression crossJoin:
                    this.VisitCrossJoin(crossJoin);
                    break;

                default: throw new NotSupportedException($"Unsupported table expression '{table.GetType().Name}'.");
            }
        }

        protected virtual void VisitCrossJoin(CrossJoinExpression crossJoin)
        {
            this.sql.Append("CROSS JOIN ");
            this.VisitTable(crossJoin.Table);
        }

        protected virtual void VisitJoin(JoinExpression join)
        {
            switch (join.JoinType)
            {
                case JoinType.Inner:
                    this.sql.Append("INNER JOIN ");
                    break;

                case JoinType.Left:
                    this.sql.Append("LEFT JOIN ");
                    break;

                default: throw new NotSupportedException($"Unsupported join type '{join.JoinType}'.");
            }

            this.VisitTable(join.Table);
            this.sql.Append(" ON ");
            this.Visit(join.Predicate);
        }

        /// <summary>
        ///     <c>[schema].[name] [alias]</c>。資料表別名不加 <c>AS</c>，因為有些資料庫（例如 Oracle）不接受。
        /// </summary>
        protected virtual void VisitTableExpression(TableExpression table)
        {
            if (table.Schema != null) this.sql.Append(this.Delimit(table.Schema)).Append(".");

            this.sql.Append(this.Delimit(table.Name)).Append(" ").Append(this.Delimit(table.Alias));
        }

        protected virtual void VisitOrdering(OrderingExpression ordering)
        {
            this.Visit(ordering.Expression);
            this.sql.Append(ordering.IsAscending ? " ASC" : " DESC");
        }

        protected virtual void Visit(SqlExpression expression)
        {
            switch (expression)
            {
                case ColumnExpression column:
                    this.VisitColumn(column);
                    break;

                case SqlParameterExpression parameter:
                    this.VisitParameter(parameter);
                    break;

                case SqlConstantExpression constant:
                    this.VisitConstant(constant);
                    break;

                case SqlBinaryExpression binary:
                    this.VisitBinary(binary);
                    break;

                case SqlUnaryExpression unary:
                    this.VisitUnary(unary);
                    break;

                case SqlFunctionExpression function:
                    this.VisitFunction(function);
                    break;

                case InExpression @in:
                    this.VisitIn(@in);
                    break;

                case LikeExpression like:
                    this.VisitLike(like);
                    break;

                case SqlFragmentExpression fragment:
                    this.sql.Append(fragment.Sql);
                    break;

                case CaseExpression @case:
                    this.VisitCase(@case);
                    break;

                default: throw new NotSupportedException($"Unsupported SQL expression '{expression.GetType().Name}'.");
            }
        }

        protected virtual void VisitColumn(ColumnExpression column)
        {
            this.sql.Append(this.Delimit(column.Table.Alias)).Append(".").Append(this.Delimit(column.Name));
        }

        /// <summary>
        ///     輸出參數佔位符號並記錄參數。同一個名稱出現多次時只記錄一次；同名但值不同代表翻譯器有錯，直接丟出例外。
        /// </summary>
        protected virtual void VisitParameter(SqlParameterExpression parameter)
        {
            var existing = this.parameters.FirstOrDefault(p => p.Name == parameter.Name);

            if (existing == null)
            {
                this.parameters.Add(parameter);
            }
            else if (!ReferenceEquals(existing, parameter) && !Equals(existing.Value, parameter.Value))
            {
                throw new InvalidOperationException($"Parameter '{parameter.Name}' is used with different values.");
            }

            this.sql.Append(this.GetParameterPlaceholder(parameter));
        }

        protected virtual void VisitConstant(SqlConstantExpression constant)
        {
            this.sql.Append(this.FormatConstant(constant.Value));
        }

        /// <summary>
        ///     運算元本身也是二元運算（含 <c>LIKE</c>）時一律加上括號，不依賴各資料庫的運算子優先順序。
        /// </summary>
        protected virtual void VisitBinary(SqlBinaryExpression binary)
        {
            this.VisitOperand(binary.Left);
            this.sql.Append(" ").Append(this.GetOperator(binary)).Append(" ");
            this.VisitOperand(binary.Right);
        }

        protected virtual void VisitUnary(SqlUnaryExpression unary)
        {
            switch (unary.Operator)
            {
                case SqlUnaryOperator.Not:
                    this.sql.Append("NOT (");
                    this.Visit(unary.Operand);
                    this.sql.Append(")");
                    break;

                case SqlUnaryOperator.Negate:
                    this.sql.Append("-");
                    this.VisitOperand(unary.Operand);
                    break;

                case SqlUnaryOperator.IsNull:
                    this.VisitOperand(unary.Operand);
                    this.sql.Append(" IS NULL");
                    break;

                case SqlUnaryOperator.IsNotNull:
                    this.VisitOperand(unary.Operand);
                    this.sql.Append(" IS NOT NULL");
                    break;

                default: throw new NotSupportedException($"Unsupported unary operator '{unary.Operator}'.");
            }
        }

        protected virtual void VisitFunction(SqlFunctionExpression function)
        {
            this.sql.Append(function.Name).Append("(");
            this.GenerateList(function.Arguments, this.Visit);
            this.sql.Append(")");
        }

        protected virtual void VisitCase(CaseExpression @case)
        {
            this.sql.Append("CASE");

            foreach (var whenClause in @case.WhenClauses)
            {
                this.sql.Append(" WHEN ");
                this.Visit(whenClause.Test);
                this.sql.Append(" THEN ");
                this.Visit(whenClause.Result);
            }

            if (@case.ElseResult != null)
            {
                this.sql.Append(" ELSE ");
                this.Visit(@case.ElseResult);
            }

            this.sql.Append(" END");
        }

        protected virtual void VisitLike(LikeExpression like)
        {
            this.VisitOperand(like.Match);
            this.sql.Append(" LIKE ");
            this.VisitOperand(like.Pattern);

            if (like.EscapeChar != null) this.sql.Append(" ESCAPE ").Append(this.FormatConstant(like.EscapeChar.Value.ToString()));
        }

        /// <summary>
        ///     值清單是空的時候，<c>IN</c> 輸出恆假的 <c>1 = 0</c>，<c>NOT IN</c> 輸出恆真的 <c>1 = 1</c>。
        /// </summary>
        protected virtual void VisitIn(InExpression @in)
        {
            if (@in.Values.Count == 0)
            {
                this.sql.Append(@in.IsNegated ? "1 = 1" : "1 = 0");

                return;
            }

            this.VisitOperand(@in.Item);
            this.sql.Append(@in.IsNegated ? " NOT IN (" : " IN (");
            this.GenerateList(@in.Values, this.Visit);
            this.sql.Append(")");
        }

        /// <summary>
        ///     ANSI 以雙引號跳脫識別字，識別字中的雙引號重複一次。
        /// </summary>
        protected virtual string Delimit(string identifier)
        {
            return string.Concat("\"", identifier.Replace("\"", "\"\""), "\"");
        }

        /// <summary>
        ///     參數在 SQL 中的寫法，預設為 <c>@name</c>。方言可以依參數的值決定其他寫法。
        /// </summary>
        protected virtual string GetParameterPlaceholder(SqlParameterExpression parameter)
        {
            return string.Concat("@", parameter.Name);
        }

        /// <summary>
        ///     ANSI 的字串串接是 <c>||</c>；其他運算子對應到通用的 SQL 符號。
        /// </summary>
        protected virtual string GetOperator(SqlBinaryExpression binary)
        {
            switch (binary.Operator)
            {
                case SqlBinaryOperator.Equal: return "=";
                case SqlBinaryOperator.NotEqual: return "<>";
                case SqlBinaryOperator.LessThan: return "<";
                case SqlBinaryOperator.LessThanOrEqual: return "<=";
                case SqlBinaryOperator.GreaterThan: return ">";
                case SqlBinaryOperator.GreaterThanOrEqual: return ">=";
                case SqlBinaryOperator.AndAlso: return "AND";
                case SqlBinaryOperator.OrElse: return "OR";
                case SqlBinaryOperator.Add: return binary.Type == typeof(string) ? "||" : "+";
                case SqlBinaryOperator.Subtract: return "-";
                case SqlBinaryOperator.Multiply: return "*";
                case SqlBinaryOperator.Divide: return "/";
                case SqlBinaryOperator.Modulo: return "%";
                case SqlBinaryOperator.BitwiseAnd: return "&";
                case SqlBinaryOperator.BitwiseOr: return "|";
                case SqlBinaryOperator.BitwiseXor: return "^";
                default: throw new NotSupportedException($"Unsupported binary operator '{binary.Operator}'.");
            }
        }

        /// <summary>
        ///     常數只支援 null、布林、字串與數值；使用者提供的值應該用參數，不應該出現在常數裡。
        /// </summary>
        protected virtual string FormatConstant(object value)
        {
            switch (value)
            {
                case null: return "NULL";
                case bool boolean: return boolean ? "TRUE" : "FALSE";
                case string str: return string.Concat("'", str.Replace("'", "''"), "'");
                case byte _:
                case sbyte _:
                case short _:
                case ushort _:
                case int _:
                case uint _:
                case long _:
                case ulong _:
                case decimal _:
                case float _:
                case double _:
                    return ((IFormattable)value).ToString(null, CultureInfo.InvariantCulture);

                default: throw new NotSupportedException($"Unsupported constant type '{value.GetType().Name}'.");
            }
        }

        private void VisitOperand(SqlExpression operand)
        {
            if (operand is SqlBinaryExpression || operand is LikeExpression)
            {
                this.sql.Append("(");
                this.Visit(operand);
                this.sql.Append(")");
            }
            else
            {
                this.Visit(operand);
            }
        }

        private void GenerateList<TItem>(IReadOnlyList<TItem> items, Action<TItem> generate)
        {
            for (var i = 0; i < items.Count; i++)
            {
                if (i > 0) this.sql.Append(", ");

                generate(items[i]);
            }
        }
    }
}
