using System;
using Rebus.SqlServer.Outbox;

namespace Rebus.Config.Outbox
{
    /// <summary>
    /// Configuration extensions for the experimental outbox support
    /// </summary>
    public static class SqlServerOutboxConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use an outbox.
        /// This will store a (message ID, source queue) tuple for all processed messages, and under this tuple any messages sent/published will
        /// also be stored, thus enabling truly idempotent message processing.
        /// </summary>
        public static RebusConfigurer Outbox(this RebusConfigurer configurer, Action<StandardConfigurer<IOutboxStorage>> configure)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (configure == null) throw new ArgumentNullException(nameof(configure));

            configurer.Options(o => configure(StandardConfigurer<IOutboxStorage>.GetConfigurerFrom(o)));

            return configurer;
        }

        public static void UseSqlServer(this StandardConfigurer<IOutboxStorage> configurer, string connectionString, string tableName)
        {

        }
    }
}
