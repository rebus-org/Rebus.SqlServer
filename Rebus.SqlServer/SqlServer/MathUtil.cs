using System;

namespace Rebus.SqlServer
{
    static class MathUtil
    {
        public static int GetNextPowerOfTwo(int input)
        {
            if (input < 0) throw new ArgumentOutOfRangeException(nameof(input), input, "Please pass a non-negative number to this function");

            if (input == 0) return 0;

            var log = Math.Log(input, 2);
            var logRoundedUp = Math.Ceiling(log);
            return (int) Math.Pow(2, logRoundedUp);
        }
    }
}