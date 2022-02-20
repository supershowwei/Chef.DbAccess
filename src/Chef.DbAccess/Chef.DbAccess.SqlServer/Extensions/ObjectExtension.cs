using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Chef.DbAccess.SqlServer.Extensions
{
    internal static class ObjectExtension
    {
        private static readonly ConcurrentDictionary<Type, Func<object, Dictionary<string, object>>> RequiredExtractors = new();

        public static IDictionary<string, object> ExtractRequired(this object me)
        {
            var converter = RequiredExtractors.GetOrAdd(
                me.GetType(),
                inputType =>
                    {
                        var requiredProperties = inputType.GetProperties()
                            .Where(p => Attribute.IsDefined(p, typeof(RequiredAttribute)))
                            .ToArray();

                        if (requiredProperties.Length == 0) throw new ArgumentException("There must be at least one [Required] column.");

                        var outputType = typeof(Dictionary<string, object>);

                        var addMethod = outputType.GetMethod(
                            "Add",
                            BindingFlags.Public | BindingFlags.Instance,
                            null,
                            new[] { typeof(string), typeof(object) },
                            null);

                        var inputParam = Expression.Parameter(typeof(object), "input");
                        var inputVariable = Expression.Convert(inputParam, inputType);
                        var outputVariable = Expression.Variable(outputType, "output");

                        var body = new List<Expression>();

                        body.Add(Expression.Assign(outputVariable, Expression.New(outputType)));

                        foreach (var property in requiredProperties)
                        {
                            // 跳過唯寫屬性及索引子
                            if (!property.CanRead || property.GetIndexParameters().Length != 0) continue;

                            var key = Expression.Constant(property.Name);
                            var value = Expression.Property(inputVariable, property);

                            var valueAsObject = Expression.Convert(value, typeof(object));

                            body.Add(Expression.Call(outputVariable, addMethod, key, valueAsObject));
                        }

                        body.Add(outputVariable);

                        var block = Expression.Block(new[] { outputVariable }, body);

                        var lambdaExpression = Expression.Lambda<Func<object, Dictionary<string, object>>>(block, inputParam);

                        return lambdaExpression.Compile();
                    });

            return converter(me);
        }
    }
}