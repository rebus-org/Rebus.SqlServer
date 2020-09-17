using System;
using Rebus.Sagas;

namespace Rebus.SqlServer.Sagas.Serialization
{
    /// <summary>
    /// Serializer used to serialize and deserialize saga data
    /// </summary>
    public interface ISagaSerializer
    {
        /// <summary>
        /// Serializes the given ISagaData object into a string
        /// </summary>
        string SerializeToString(ISagaData obj);

        /// <summary>
        /// Deserializes the given string and type into a ISagaData object
        /// </summary>
        ISagaData DeserializeFromString(Type type, string str);
    }
}
