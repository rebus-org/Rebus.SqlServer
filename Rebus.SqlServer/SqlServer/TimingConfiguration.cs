using System;

namespace Rebus.SqlServer
{
    class TimingConfiguration
    {
        public TimeSpan ExpiredMessagesCleanupInterval { get; set; } = TimeSpan.FromMinutes(1);
    }
}