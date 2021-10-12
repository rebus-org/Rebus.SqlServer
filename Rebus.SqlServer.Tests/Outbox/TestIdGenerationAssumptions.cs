using System;
using System.Collections.Generic;
using System.Globalization;
using NUnit.Framework;

namespace Rebus.SqlServer.Tests.Outbox;

[TestFixture]
public class TestIdGenerationAssumptions
{
    [TestCase(4)]
    [TestCase(5)]
    [TestCase(6)]
    [TestCase(7)]
    [TestCase(8)]
    [TestCase(16)]
    [Repeat(5)]
    public void CheckGuidPrefix(int length) => RunTest(() => Guid.NewGuid().ToString("N").Substring(0, length));

    [Test]
    [Repeat(5)]
    public void CheckTimestampHashCode() => RunTest(() => DateTime.Now.GetHashCode().ToString(CultureInfo.InvariantCulture));

    [Test]
    public void CheckLengthOfIntegers()
    {
        Console.WriteLine(int.MaxValue);
        Console.WriteLine(int.MaxValue.ToString().Length);
    }

    static void RunTest(Func<string> getNextId)
    {
        var ids = new HashSet<string>();

        while (true)
        {
            var id = getNextId();

            if (ids.Contains(id))
            {
                Console.WriteLine($"The ID {id} already exists - found {ids.Count} unique ids");
                return;
            }

            ids.Add(id);

            if (ids.Count == 1000 * 1000)
            {
                Console.WriteLine("OK that was 1M - quitting");
                return;
            }
        }
    }
}
