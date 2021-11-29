using NUnit.Framework;
using Rebus.SqlServer.Outbox;

namespace Rebus.SqlServer.Tests.Outbox;

[TestFixture]
public class TestMathExtensions
{
    [TestCase(1, 1)]
    [TestCase(2, 2)]
    [TestCase(3, 4)]
    [TestCase(4, 4)]
    [TestCase(5, 8)]
    [TestCase(10, 16)]
    [TestCase(17, 32)]
    [TestCase(789, 1024)]
    public void CanRoundUpToNextPowerOfTwo(int input, int expectedOutput) => Assert.That(input.RoundUpToNextPowerOfTwo(), Is.EqualTo(expectedOutput));
}
