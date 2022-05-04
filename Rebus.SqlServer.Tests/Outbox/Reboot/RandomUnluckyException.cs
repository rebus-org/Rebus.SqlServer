using System;

namespace Rebus.SqlServer.Tests.Outbox.Reboot;

class RandomUnluckyException : ApplicationException
{
    public RandomUnluckyException() : base("You were unfortunate")
    {
    }
}