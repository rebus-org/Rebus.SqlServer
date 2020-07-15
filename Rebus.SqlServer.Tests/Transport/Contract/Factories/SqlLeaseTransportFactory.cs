﻿using System;
using System.Collections.Generic;

using Rebus.Config;
using Rebus.Logging;
using Rebus.SqlServer.Transport;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Transports;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.SqlServer.Tests.Transport.Contract.Factories
{
    public class SqlLeaseTransportFactory : ITransportFactory
    {
        readonly HashSet<string> _tablesToDrop = new HashSet<string>();
        readonly List<IDisposable> _disposables = new List<IDisposable>();

        public SqlLeaseTransportFactory()
        {
            SqlTestHelper.DropAllTables();
        }

        public ITransport CreateOneWayClient()
        {
            var tableName = ("RebusMessages_" + TestConfig.Suffix).TrimEnd('_');

            SqlTestHelper.DropTable(tableName);

            _tablesToDrop.Add(tableName);

            var rebusTime = new DefaultRebusTime();
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);
            var transport = new SqlServerLeaseTransport(connectionProvider, null, consoleLoggerFactory,
                asyncTaskFactory, rebusTime, TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(2), () => Environment.MachineName, new SqlServerLeaseTransportOptions(connectionProvider));

            _disposables.Add(transport);

            transport.Initialize();

            return transport;
        }

        public ITransport Create(string inputQueueAddress)
        {
            var tableName = ("RebusMessages_" + TestConfig.Suffix).TrimEnd('_');

            SqlTestHelper.DropTable(tableName);

            _tablesToDrop.Add(tableName);

            var rebusTime = new DefaultRebusTime();
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionProvider = new DbConnectionProvider(SqlTestHelper.ConnectionString, consoleLoggerFactory);
            var asyncTaskFactory = new TplAsyncTaskFactory(consoleLoggerFactory);
            var transport = new SqlServerLeaseTransport(connectionProvider, inputQueueAddress, consoleLoggerFactory,
                asyncTaskFactory, rebusTime, TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(2), () => Environment.MachineName, new SqlServerLeaseTransportOptions(connectionProvider));

            _disposables.Add(transport);

            transport.EnsureTableIsCreated();
            transport.Initialize();

            return transport;
        }

        public void CleanUp()
        {
            _disposables.ForEach(d => d.Dispose());
            _disposables.Clear();

            foreach (var table in _tablesToDrop)
            {
                SqlTestHelper.DropTable(table);
            }

            _tablesToDrop.Clear();
        }
    }
}
