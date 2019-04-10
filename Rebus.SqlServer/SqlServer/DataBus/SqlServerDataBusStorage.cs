using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.DataBus;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Serialization;
using Rebus.Time;
// ReSharper disable SimplifyLinqExpression

namespace Rebus.SqlServer.DataBus
{
    /// <summary>
    /// Implementation of <see cref="IDataBusStorage"/> that uses SQL Server to store data
    /// </summary>
    public class SqlServerDataBusStorage : IDataBusStorage, IInitializable
    {
        static readonly Encoding TextEncoding = Encoding.UTF8;
        readonly DictionarySerializer _dictionarySerializer = new DictionarySerializer();
        readonly IDbConnectionProvider _connectionProvider;
        readonly TableName _tableName;
        readonly bool _ensureTableIsCreated;
        readonly ILog _log;
        readonly int _commandTimeout ;
        private readonly IRebusTime _rebusTime;

        /// <summary>
        /// Creates the data storage
        /// </summary>
        public SqlServerDataBusStorage(IDbConnectionProvider connectionProvider, string tableName, bool ensureTableIsCreated, IRebusLoggerFactory rebusLoggerFactory, IRebusTime rebusTime, int commandTimeout)
        {
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
            _tableName = TableName.Parse(tableName);
            _ensureTableIsCreated = ensureTableIsCreated;
            _commandTimeout = commandTimeout;
            _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
            _log = rebusLoggerFactory.GetLogger<SqlServerDataBusStorage>();
        }

        /// <summary>
        /// Initializes the SQL Server data storage.
        /// Will create the data table, unless this has been explicitly turned off when configuring the data storage
        /// </summary>
        public void Initialize()
        {
            if (!_ensureTableIsCreated) return;

            try
            {
                AsyncHelpers.RunSync(EnsureTableIsCreatedAsync);
            }
            catch
            {
                // if it failed because of a collision between another thread doing the same thing, just try again once:
                AsyncHelpers.RunSync(EnsureTableIsCreatedAsync);
            }

        }

        async Task EnsureTableIsCreatedAsync()
        {
            using (var connection = await _connectionProvider.GetConnection())
            {

                if (connection.GetTableNames().Contains(_tableName))
                {
                    var columns = connection.GetColumns(_tableName.Schema, _tableName.Name);
                    
                    if (!columns.Any(x => x.Name == "CreationTime"))
                    {
                        _log.Info("Adding CreationTime column to data bus table {tableName}", _tableName.QualifiedName);

                        using (var command = connection.CreateCommand())
                        {
                            command.CommandText = $@"
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE Name = N'CreationTime' AND Object_ID = Object_ID(N'{_tableName.QualifiedName}'))
BEGIN
    ALTER TABLE {_tableName.QualifiedName} ADD [CreationTime] DATETIMEOFFSET
END
";
                            command.ExecuteNonQuery();
                        }

                        await connection.Complete();
                    }

                    return;
                }

                _log.Info("Creating data bus table {tableName}", _tableName.QualifiedName);

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{_tableName.Schema}')
	EXEC('CREATE SCHEMA {_tableName.Schema}')

----

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{_tableName.Schema}' AND TABLE_NAME = '{_tableName.Name}')
    CREATE TABLE {_tableName.QualifiedName} (
        [Id] VARCHAR(200),
        [Meta] VARBINARY(MAX),
        [Data] VARBINARY(MAX),
        [CreationTime] DATETIMEOFFSET,
        [LastReadTime] DATETIMEOFFSET
    );

";
                    const int tableAlreadyExists = 2714;

                    try
                    {
                        command.ExecuteNonQuery();
                    }
                    catch (SqlException exception) when (exception.Number == tableAlreadyExists)
                    {
                        // table already exists - just quit now
                        return;
                    }
                }

                await connection.Complete();
            }
        }

        /// <summary>
        /// Saves the data from the given source stream under the given ID
        /// </summary>
        public async Task Save(string id, Stream source, Dictionary<string, string> metadata = null)
        {
            var metadataToWrite = new Dictionary<string, string>(metadata ?? new Dictionary<string, string>())
            {
                [MetadataKeys.SaveTime] = _rebusTime.Now.ToString("O")
            };

            try
            {
                using (var connection = await _connectionProvider.GetConnection())
                {
                    using (var command = connection.CreateCommand())
                    {
                        var metadataBytes = TextEncoding.GetBytes(_dictionarySerializer.SerializeToString(metadataToWrite));

                        command.CommandTimeout = _commandTimeout;
                        command.CommandText = $"INSERT INTO {_tableName.QualifiedName} ([Id], [Meta], [Data], [CreationTime]) VALUES (@id, @meta, @data, @now)";
                        command.Parameters.Add("id", SqlDbType.VarChar, 200).Value = id;
                        command.Parameters.Add("meta", SqlDbType.VarBinary, MathUtil.GetNextPowerOfTwo(metadataBytes.Length)).Value = metadataBytes;
                        command.Parameters.Add("data", SqlDbType.VarBinary, MathUtil.GetNextPowerOfTwo((int)source.Length)).Value = source;
                        command.Parameters.Add("now", SqlDbType.DateTimeOffset).Value = _rebusTime.Now;

                        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }

                    await connection.Complete();
                }
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not save data with ID {id}");
            }
        }

        /// <summary>
        /// Opens the data stored under the given ID for reading
        /// </summary>
        public async Task<Stream> Read(string id)
        {
            try
            {
                // update last read time quickly
                await UpdateLastReadTime(id);

                var connection = await _connectionProvider.GetConnection();

                using (var command = connection.CreateCommand())
                {
                    try
                    {
                        command.CommandTimeout = _commandTimeout;
                        command.CommandText = $"SELECT TOP 1 [Data] FROM {_tableName.QualifiedName} WITH (NOLOCK) WHERE [Id] = @id";
                        command.Parameters.Add("id", SqlDbType.VarChar, 200).Value = id;

                        var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess).ConfigureAwait(false);

                        if (!await reader.ReadAsync().ConfigureAwait(false))
                        {
                            throw new ArgumentException($"Row with ID {id} not found");
                        }

                        var dataOrdinal = reader.GetOrdinal("data");
                        var stream = reader.GetStream(dataOrdinal);

                        return new StreamWrapper(stream, new IDisposable[]
                        {
                            // defer closing these until the returned stream is closed
                            reader,
                            connection
                        });
                    }
                    catch
                    {
                        // if something of the above fails, we did not pass the connection to someone who can dispose it... wherefore:
                        connection.Dispose();
                        throw;
                    }
                }
            }
            catch (ArgumentException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not load data with ID {id}");
            }
        }

        async Task UpdateLastReadTime(string id)
        {
            using (var connection = await _connectionProvider.GetConnection())
            {
                await UpdateLastReadTime(id, connection).ConfigureAwait(false);
                await connection.Complete();
            }
        }

        async Task UpdateLastReadTime(string id, IDbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"UPDATE {_tableName.QualifiedName} SET [LastReadTime] = @now WHERE [Id] = @id";
                command.Parameters.Add("now", SqlDbType.DateTimeOffset).Value = _rebusTime.Now;
                command.Parameters.Add("id", SqlDbType.VarChar, 200).Value = id;
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Loads the metadata stored with the given ID
        /// </summary>
        public async Task<Dictionary<string, string>> ReadMetadata(string id)
        {
            try
            {
                using (var connection = await _connectionProvider.GetConnection())
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = $"SELECT TOP 1 [Meta], [LastReadTime], DATALENGTH([Data]) AS 'Length' FROM {_tableName.QualifiedName} WITH (NOLOCK) WHERE [Id] = @id";
                        command.Parameters.Add("id", SqlDbType.VarChar, 200).Value = id;

                        using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                        {
                            if (!await reader.ReadAsync().ConfigureAwait(false))
                            {
                                throw new ArgumentException($"Row with ID {id} not found");
                            }

                            var bytes = (byte[])reader["Meta"];
                            var length = (long)reader["Length"];
                            var lastReadTimeDbValue = reader["LastReadTime"];

                            var jsonText = TextEncoding.GetString(bytes);
                            var metadata = _dictionarySerializer.DeserializeFromString(jsonText);

                            metadata[MetadataKeys.Length] = length.ToString();

                            if (lastReadTimeDbValue != DBNull.Value)
                            {
                                var lastReadTime = (DateTimeOffset)lastReadTimeDbValue;

                                metadata[MetadataKeys.ReadTime] = lastReadTime.ToString("O");
                            }

                            return metadata;
                        }
                    }
                }
            }
            catch (ArgumentException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not load metadata for data with ID {id}");
            }
        }
    }
}