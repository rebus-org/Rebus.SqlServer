using System;
using System.Collections.Generic;
using System.Text;
using Rebus.Config;
using Rebus.Sagas;

namespace Rebus.SqlServer.Sagas.Serialization
{

    /// <summary>
    /// Configuration extensions SQL Server Saga Serializer
    /// </summary>
    public static class SagaSerializerConfigurationExtensions
    {
        /// <summary>
        /// Configures saga to use your own custom saga serializer
        /// </summary>
        public static void UseSagaSerializer(this StandardConfigurer<ISagaStorage> configurer, ISagaSerializer serializer = null)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (serializer == null)
            {
                serializer = new DefaultSagaSerializer();
            }

            RegisterSerializer(configurer, serializer);
        }

        static void RegisterSerializer(StandardConfigurer<ISagaStorage> configurer, ISagaSerializer serializer)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if(serializer == null)
            {
                serializer = new DefaultSagaSerializer();
            }

            configurer.OtherService<ISagaSerializer>().Decorate((c) => serializer);
        }
    }
}
