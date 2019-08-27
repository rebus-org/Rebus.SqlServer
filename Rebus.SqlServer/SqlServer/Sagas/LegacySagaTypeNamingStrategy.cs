using System;

namespace Rebus.SqlServer.Sagas
{
    /// <summary>
    /// Implementation of <seealso cref="ISagaTypeNamingStrategy"/> which uses legacy type naming; simply returning the name of the class
    /// </summary>
    public class LegacySagaTypeNamingStrategy : ISagaTypeNamingStrategy
    {
        /// <inheritdoc />
        public string GetSagaTypeName(Type sagaDataType, int maximumLength)
        {
            return sagaDataType.Name;
        }
    }
}
