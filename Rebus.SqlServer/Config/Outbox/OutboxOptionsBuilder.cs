using System;

namespace Rebus.Config.Outbox;

/// <summary>
/// Builder things that allows for configuring outbox options
/// </summary>
public class OutboxOptionsBuilder
{
    /// <summary>
    /// Sets the delay between checking the outbox for pending messages to be sent. Setting this <paramref name="delay"/> will affect
    /// how long it will take for the background process to discover that messages previously committed to the outbox were not sent/published
    /// as they should have been and thus retry the send/publish operations. Defaults to 5 seconds.
    /// </summary>
    public OutboxOptionsBuilder SetMessageForwarderDelay(TimeSpan delay)
    {
        MessageForwarderDelay = delay;
        return this;
    }

    /// <summary>
    /// Sets the delay between checking the outbox for things to clean up. Defaults to 2 minutes.
    /// </summary>
    /// <param name="delay"></param>
    /// <returns></returns>
    public OutboxOptionsBuilder SetCleanupDelay(TimeSpan delay)
    {
        CleanupDelay = delay;
        return this;
    }

    /// <summary>
    /// Sets how long to keep idempotency marks in the outbox. Setting this affects the duration of the window within which a re-delivered
    /// message can be detected. Defaults to 24 hours.
    /// </summary>
    public OutboxOptionsBuilder SetIdempotencyTimeout(TimeSpan timeout)
    {
        IdempotencyTimeout = timeout;
        return this;
    }

    TimeSpan MessageForwarderDelay { get; set; } = TimeSpan.FromSeconds(5);
    TimeSpan CleanupDelay { get; set; } = TimeSpan.FromMinutes(2);
    TimeSpan IdempotencyTimeout { get; set; } = TimeSpan.FromDays(1);

    internal int GetMessageForwarderDelaySeconds()
    {
        if (MessageForwarderDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(MessageForwarderDelay), MessageForwarderDelay,
                "Please set the delay to a value >= 0 seconds");
        }

        return (int)MessageForwarderDelay.TotalSeconds;
    }

    internal int GetCleanupDelaySeconds()
    {
        if (CleanupDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(CleanupDelay), CleanupDelay,
                "Please set the delay to a value >= 0 seconds");
        }

        return (int)CleanupDelay.TotalSeconds;
    }

    internal TimeSpan GetIdempotencyTimeout()
    {
        if (IdempotencyTimeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(IdempotencyTimeout), IdempotencyTimeout,
                "Please set the delay to a value >= 0 seconds");
        }

        return IdempotencyTimeout;
    }
}