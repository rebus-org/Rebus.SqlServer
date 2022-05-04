using System;

namespace Rebus.SqlServer.Tests.Outbox;

class RandomUnluckyException : ApplicationException
{
    public RandomUnluckyException() : base("You were unfortunate")
    {
    }
}