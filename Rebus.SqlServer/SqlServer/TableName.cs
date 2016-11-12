using System;
using System.Linq;

namespace Rebus.SqlServer
{
    public class TableName: IEquatable<TableName>
    {
        public string Schema { get; }
        public string Name { get; }

        internal string QualifiedName => $"[{Schema}].[{Name}]";

        public TableName(string schema, string tableName)
        {
            Schema = StripBrackets(schema);
            Name = StripBrackets(tableName);
        }

        public TableName(string tableName)
        {
            if (tableName.Contains("."))
            {
                var splitted = tableName.Split(new[] {'.'}, StringSplitOptions.RemoveEmptyEntries);
                var schema = splitted[0];
                var name = string.Join(".", splitted.Skip(1));

                Schema = StripBrackets(schema);
                Name = StripBrackets(name);
            }
            else
            {
                Schema = "dbo";
                Name = StripBrackets(tableName);
            }
        }

        private string StripBrackets(string value)
        {
            if (value.StartsWith("["))
            {
                value = value.Substring(1);
            }
            if (value.EndsWith("]"))
            {
                value = value.Substring(0, value.Length - 1);
            }

            return value;
        }

        public bool Equals(TableName other)
        {
            if (other == null)
                return false;

            return
                string.Equals(Schema, other.Schema, StringComparison.CurrentCultureIgnoreCase) &&
                string.Equals(Name, other.Name, StringComparison.CurrentCultureIgnoreCase);
        }

        public override int GetHashCode()
        {
            return Schema.GetHashCode() + Name.GetHashCode();
        }
    }
}
