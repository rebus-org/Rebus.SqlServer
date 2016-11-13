using System;
using System.Linq;

namespace Rebus.SqlServer
{
    /// <summary>
    /// Represents a (possibly schema-qualified) table name in SQL Server
    /// </summary>
    public class TableName : IEquatable<TableName>
    {
        /// <summary>
        /// Gets the schema name of the table
        /// </summary>
        public string Schema { get; }
        
        /// <summary>
        /// Gets the table's name
        /// </summary>
        public string Name { get; }

        internal string QualifiedName => $"[{Schema}].[{Name}]";

        /// <summary>
        /// Creates a <see cref="TableName"/> object with the given schema and table names
        /// </summary>
        public TableName(string schema, string tableName)
        {
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            Schema = StripBrackets(schema);
            Name = StripBrackets(tableName);
        }

        /// <summary>
        /// Parses the given name into a <see cref="TableName"/>, defaulting to using the 'dbo' schema unless the name is schema-qualified.
        /// E.g. 'table' will result in a <see cref="TableName"/> representing the '[dbo].[table]' table, whereas 'accounting.messages' will
        /// represent the '[accounting].[messages]' table.
        /// </summary>
        public static TableName Parse(string name)
        {
            return new TableName(name);
        }

        TableName(string tableName)
        {
            if (tableName.Contains("."))
            {
                var splitted = tableName.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
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

        string StripBrackets(string value)
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


        /// <inheritdoc />
        public bool Equals(TableName other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Schema, other.Schema) && string.Equals(Name, other.Name);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((TableName) obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                return (Schema.GetHashCode() * 397) ^ Name.GetHashCode();
            }
        }

        /// <summary>
        /// Checks whether the two <see cref="TableName"/> objects are equal (i.e. represent the same table)
        /// </summary>
        public static bool operator ==(TableName left, TableName right)
        {
            return Equals(left, right);
        }

        /// <summary>
        /// Checks whether the two <see cref="TableName"/> objects are not equal (i.e. do not represent the same table)
        /// </summary>
        public static bool operator !=(TableName left, TableName right)
        {
            return !Equals(left, right);
        }
    }
}
