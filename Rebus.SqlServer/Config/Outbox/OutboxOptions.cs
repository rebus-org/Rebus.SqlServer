using System;

namespace Rebus.Config.Outbox;

/// <summary>
/// Contains configuration options for the outbox
/// </summary>
public class OutboxOptions
{
    int _forwarderIntervalSeconds = 1;

    /// <summary>
    /// Gets or sets the interval in seconds between outbox forwarding runs. Default is 1. Cannot be less than 1.
    /// </summary>
    public int ForwarderIntervalSeconds
    {
        get => _forwarderIntervalSeconds;
        set
        {
            if (value < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "The forwarder interval must be at least 1 second.");
            }
            _forwarderIntervalSeconds = value;
        }
    }

    /// <summary>
    /// Gets or sets whether to run the outbox forwarder background task. Default is true.
    /// </summary>
    public bool RunForwarder { get; set; } = true;
}
