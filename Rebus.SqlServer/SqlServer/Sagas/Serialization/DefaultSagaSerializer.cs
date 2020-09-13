using System;
using Rebus.Sagas;
using Rebus.Serialization;

namespace Rebus.SqlServer.Sagas.Serialization
{
    /// <summary>
    /// The default serializer for serializing sql saga data,
    /// Implement <seealso cref="ISagaSerializer"/> to make your own custom serializer and register it using the UseSagaSerializer extension method.
    /// <seealso cref="Rebus.Config.SqlServerSagaConfigurationExtensions.UseSagaSerializer"/>
    /// </summary>
    public class DefaultSagaSerializer : ObjectSerializer, ISagaSerializer
    {
        /// <summary>
        /// Serializes the given ISagaData object into a string
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public string SerializeToString(ISagaData obj)
        {
            return base.SerializeToString(obj);
        }

        /// <summary>
        /// Deserializes the given string and type into a ISagaData object
        /// </summary>
        /// <param name="type"></param>
        /// <param name="str"></param>
        /// <returns></returns>
        public ISagaData DeserializeFromString(Type type, string str)
        {
            var sagaData = base.DeserializeFromString(str) as ISagaData;

            if (!type.IsInstanceOfType(sagaData))
            {
                return null;
            }
            return sagaData;
        }
    }
}
