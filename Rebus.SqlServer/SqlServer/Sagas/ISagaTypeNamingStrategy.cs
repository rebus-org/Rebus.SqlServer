using System;

namespace Rebus.SqlServer.Sagas
{
    /// <summary>
    /// A contract for generating a type name for a saga as it'll be stored in the database
    /// </summary>
    public interface ISagaTypeNamingStrategy
    {
        /// <summary>
        /// Generate a saga type name. Implementations should be pure/stable; for a given input the output should always be the same as it's used to find the saga in the database
        /// </summary>
        /// <param name="sagaDataType">Type a name is to be generated for</param>
        /// <param name="maximumLength">Maximum allowed length of the result. If the return is longer than this an exception will be thrown</param>
        /// <returns>A string representation of <paramref name="sagaDataType"/></returns>
        string GetSagaTypeName(Type sagaDataType, int maximumLength);
    }
}
