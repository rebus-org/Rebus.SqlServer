using NUnit.Framework;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;

namespace Rebus.SqlServer.Tests.Assumptions;

[TestFixture]
public class TestUintBuckets : FixtureBase
{
    [TestCase(10)]
    [TestCase(100)]
    [TestCase(128)]
    [TestCase(256)]
    public void GetSomeBucketNumbers(int bucketCount)
    {
        10000.Times(() => Assert.That(ConnectionLocker.GetIntBucket(new object(), bucketCount), Is.GreaterThanOrEqualTo(0).And.LessThan(bucketCount)));
    }
}