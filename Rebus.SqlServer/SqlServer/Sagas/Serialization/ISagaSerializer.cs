using Rebus.Sagas;

namespace Rebus.SqlServer.Sagas.Serialization
{
    /// <summary>
    /// Serializer used to serialize and deserialize saga data
    /// </summary>
    public interface ISagaSerializer
    {
        /// <summary>
        /// Serializes the given object into a string
        /// </summary>
        string SerializeToString(ISagaData obj);

        /// <summary>
        /// Deserializes the given string into an object
        /// </summary>
        ISagaData DeserializeFromString(string str);
    }
}
