using System;
using System.Threading.Tasks;
using Rebus.Injection;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.SqlServer;
using Rebus.SqlServer.Sagas;
using Rebus.SqlServer.Sagas.Serialization;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for sagas
    /// </summary>
    public static class SqlServerSagaConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use SQL Server to store sagas, using the tables specified to store data and indexed properties respectively.
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<ISagaStorage> configurer,
            string connectionString, string dataTableName, string indexTableName,
            bool automaticallyCreateTables = true, bool enlistInAmbientTransaction = false
    )
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
            if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
            if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionProvider(connectionString, rebusLoggerFactory, enlistInAmbientTransaction);
                var sagaTypeNamingStrategy = GetSagaTypeNamingStrategy(c, rebusLoggerFactory);
                var serializer = c.Has<ISagaSerializer>() ? c.Get<ISagaSerializer>() : new DefaultSagaSerializer();

                var sagaStorage = new SqlServerSagaStorage(connectionProvider, dataTableName, indexTableName, rebusLoggerFactory, sagaTypeNamingStrategy, serializer);

                if (automaticallyCreateTables)
                {
                    sagaStorage.EnsureTablesAreCreated();
                }

                return sagaStorage;
            });
        }

        /// <summary>
        /// Configures Rebus to use SQL Server to store sagas, using the tables specified to store data and indexed properties respectively.
        /// </summary>
        public static void StoreInSqlServer(this StandardConfigurer<ISagaStorage> configurer,
            Func<Task<IDbConnection>> connectionFactory, string dataTableName, string indexTableName,
            bool automaticallyCreateTables = true)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
            if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
            if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var connectionProvider = new DbConnectionFactoryProvider(connectionFactory, rebusLoggerFactory);
                var sagaTypeNamingStrategy = GetSagaTypeNamingStrategy(c, rebusLoggerFactory);
                var serializer = c.Has<ISagaSerializer>() ? c.Get<ISagaSerializer>() : new DefaultSagaSerializer();

                var sagaStorage = new SqlServerSagaStorage(connectionProvider, dataTableName, indexTableName, rebusLoggerFactory, sagaTypeNamingStrategy, serializer);

                if (automaticallyCreateTables)
                {
                    sagaStorage.EnsureTablesAreCreated();
                }

                return sagaStorage;
            });
        }

        /// <summary>
        /// Get the registered implementation of <seealso cref="ISagaTypeNamingStrategy"/> or the default <seealso cref="LegacySagaTypeNamingStrategy"/> if one is not configured
        /// </summary>
        private static ISagaTypeNamingStrategy GetSagaTypeNamingStrategy(IResolutionContext resolutionContext, IRebusLoggerFactory rebusLoggerFactory) 
        {
            ISagaTypeNamingStrategy sagaTypeNamingStrategy;
            if (resolutionContext.Has<ISagaTypeNamingStrategy>() == false)
            {
                rebusLoggerFactory.GetLogger<SqlServerSagaStorage>().Debug($"An implementation of {nameof(ISagaTypeNamingStrategy)} was not registered. A default, backward compatible, implementation will be used ({nameof(LegacySagaTypeNamingStrategy)}).");
                sagaTypeNamingStrategy = new LegacySagaTypeNamingStrategy();
            }
            else
            {
                sagaTypeNamingStrategy = resolutionContext.Get<ISagaTypeNamingStrategy>();
            }

            return sagaTypeNamingStrategy;
        }

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

            configurer.OtherService<ISagaSerializer>().Decorate((c) => serializer);
        }
    }
}
