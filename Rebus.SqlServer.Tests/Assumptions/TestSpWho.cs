using System;
using System.Linq;
using NUnit.Framework;

namespace Rebus.SqlServer.Tests.Assumptions
{
    [TestFixture]
    [Ignore("run if you must")]
    public class TestSpWho
    {
        [Test]
        public void DropTableThatDoesNotExist()
        {
            SqlTestHelper.DropTable("bimse");
        }

        [Test]
        public void CanGetActiveConnections()
        {
            var who = SqlTestHelper.ExecSpWho();

            Console.WriteLine(string.Join(Environment.NewLine,
                who.Select(d => string.Join(", ", d.Select(kvp => $"{kvp.Key} = {kvp.Value}")))));

        }
    }
}