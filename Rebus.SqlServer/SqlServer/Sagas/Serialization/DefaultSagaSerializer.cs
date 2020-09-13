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
        public string SerializeToString(ISagaData obj)
        {
            return base.SerializeToString(obj);
        }

        public ISagaData DeserializeFromString(string str)
        {
            return (ISagaData)base.DeserializeFromString(str);
        }
    }
}
