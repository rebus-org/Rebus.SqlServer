using System;
using System.Diagnostics;

namespace Rebus.SqlServer.Tests.Extensions
{
    static class StopWatchExtensions
    {
        public static TimeSpan GetLapTime(this Stopwatch stopwatch)
        {
            if (stopwatch == null) throw new ArgumentNullException(nameof(stopwatch));

            var elapsed = stopwatch.Elapsed;
            stopwatch.Restart();
            return elapsed;
        }    
    }
}