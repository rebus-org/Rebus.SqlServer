using System;
using Rebus.Extensions;

namespace Rebus.SqlServer.Sagas
{
    /// <summary>
    /// Returns a string that uses a portion of the actual saga type name and appends the hash of the simple assembly qualified name of the hash. This is a balance between uniqueness and readable for diagnostic purposes
    /// </summary>
    public class HumanReadableHashedSagaTypeNamingStrategy : Sha512SagaTypeNamingStrategy
    {
        /// <summary>
        /// The number of bytes to leave as human readable if no value is specified
        /// </summary>
        public const int DefaultHumanReadableBytes = 10;

        private readonly int _numberOfHumanReadableBytes;

        /// <summary>
        /// Default constructor configured to use <seealso cref="DefaultHumanReadableBytes"/> number of human readable bytes
        /// </summary>
        public HumanReadableHashedSagaTypeNamingStrategy() : this(DefaultHumanReadableBytes)
        {
        }

        /// <summary>
        /// Constructs a new instance which uses <paramref name="numberOfHumanReadableBytes"/> of human readable bytes in the output
        /// </summary>
        public HumanReadableHashedSagaTypeNamingStrategy(int numberOfHumanReadableBytes)
        {
            _numberOfHumanReadableBytes = numberOfHumanReadableBytes;
        }

        /// <inheritdoc />
        public override string GetSagaTypeName(Type sagaDataType, int maximumLength)
        {
            if (_numberOfHumanReadableBytes > maximumLength)
            {
                throw new ArgumentOutOfRangeException(nameof(maximumLength), $"Cannot generate a name of {maximumLength} bytes as {nameof(HumanReadableHashedSagaTypeNamingStrategy)} is configured to use at {_numberOfHumanReadableBytes} human readable bytes");
            }

            return GenerateHumanReadableHash(sagaDataType.Name, sagaDataType, maximumLength);
        }

        /// <summary>
        /// Generates a human readable hash that uses portions of <paramref name="humanReadableName"/> and the hash of <paramref name="sagaDataType"/>
        /// </summary>
        protected virtual string GenerateHumanReadableHash(string humanReadableName, Type sagaDataType, int maximumLength)
        {
            var humanReadablePortion = humanReadableName.Substring(0, Math.Min(_numberOfHumanReadableBytes, humanReadableName.Length));
            var simpleName = sagaDataType.GetSimpleAssemblyQualifiedName();
            var hash = GetBase64EncodedPartialHash(simpleName, maximumLength - humanReadablePortion.Length, System.Text.Encoding.UTF8);

            return $"{humanReadablePortion}{hash}";
        }
    }
}
