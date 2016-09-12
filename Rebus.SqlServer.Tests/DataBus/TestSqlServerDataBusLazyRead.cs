using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.DataBus;
using Rebus.Tests.Contracts;

namespace Rebus.SqlServer.Tests.DataBus
{
    [TestFixture]
    public class TestSqlServerDataBusLazyRead : FixtureBase
    {
        IDataBusStorage _storage;
        SqlServerDataBusStorageFactory _factory;

        protected override void SetUp()
        {
            _factory = new SqlServerDataBusStorageFactory();
            _storage = _factory.Create();
        }

        protected override void TearDown()
        {
            _factory.CleanUp();
        }

        [TestCase(1024*1024*10)]
        public async Task ReadingIsLazy(int byteCount)
        {
            const string dataId = "known id";

            Console.WriteLine("Generating data...");

            var data = GenerateData(byteCount);

            Console.WriteLine("Saving data...");

            await _storage.Save(dataId, new MemoryStream(data));

            Console.WriteLine("Reading data...");

            var stopwatch = Stopwatch.StartNew();
            using (var source = await _storage.Read(dataId))
            using (var destination = new MemoryStream())
            {
                Console.WriteLine($"Opening stream took {stopwatch.Elapsed.TotalSeconds:0.00} s");

                await source.CopyToAsync(destination);

                Console.WriteLine($"Entire operation took {stopwatch.Elapsed.TotalSeconds:0.00} s");
            }

        }

        static byte[] GenerateData(int byteCount)
        {
            var buffer = new byte[byteCount];
            new Random(DateTime.Now.GetHashCode()).NextBytes(buffer);
            return buffer;
        }
    }
}