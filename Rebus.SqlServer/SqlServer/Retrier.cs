using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.SqlServer;

/// <summary>
/// Mini-Polly 🙂
/// </summary>
class Retrier
{
    readonly List<TimeSpan> _delays;

    public Retrier(IEnumerable<TimeSpan> delays)
    {
        if (delays == null)
        {
            throw new ArgumentNullException(nameof(delays));
        }

        _delays = delays.ToList();
    }

    public async Task ExecuteAsync(Func<Task> execute, CancellationToken cancellationToken = default)
    {
        for (var index = 0; index <= _delays.Count; index++)
        {
            try
            {
                await execute();
                return;
            }
            catch (Exception exception)
            {
                if (index == _delays.Count)
                {
                    throw;
                }

                var delay = _delays[index];

                try
                {
                    await Task.Delay(delay, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
            }
        }
    }
}
