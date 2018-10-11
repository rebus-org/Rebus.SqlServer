using NUnit.Framework;

namespace Rebus.SqlServer.Tests.Math
{
    [TestFixture]
    public class TestMathUtil
    {
        [TestCase(0, 0)]
        [TestCase(1, 1)]
        [TestCase(2, 2)]
        [TestCase(3, 4)]
        [TestCase(4, 4)]
        [TestCase(5, 8)]
        [TestCase(6, 8)]
        [TestCase(7, 8)]
        [TestCase(8, 8)]
        [TestCase(9, 16)]
        [TestCase(10, 16)]
        [TestCase(15, 16)]
        [TestCase(16, 16)]
        [TestCase(17, 32)]
        [TestCase(31, 32)]
        [TestCase(32, 32)]
        [TestCase(33, 64)]
        [TestCase(63, 64)]
        [TestCase(64, 64)]
        [TestCase(65, 128)]
        [TestCase(127, 128)]
        [TestCase(128, 128)]
        [TestCase(129, 256)]
        [TestCase(255, 256)]
        [TestCase(256, 256)]
        [TestCase(257, 512)]
        public void CanGetNextPowerOfTwo_Zero(int input, int expectedOutput)
        {
            var actualOutput = MathUtil.GetNextPowerOfTwo(input);

            Assert.That(actualOutput, Is.EqualTo(expectedOutput));
            Assert.That(actualOutput, Is.GreaterThanOrEqualTo(input), 
                "Just to be sure that, if we choose to use this function to come up with a sensible, non-query plan cache-polluting length to pass as SQL parameter length, we never end up truncating anything");
        }
    }
}