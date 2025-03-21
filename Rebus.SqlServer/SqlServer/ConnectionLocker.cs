using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.SqlServer;

class ConnectionLocker(int buckets) : IDisposable
{
    public static readonly ConnectionLocker Instance = new(buckets: 256);

    readonly ConcurrentDictionary<int, SemaphoreSlim> _semaphores = new();

    public async ValueTask<IDisposable> GetLockAsync(IDbConnection connection)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var semaphore = GetSemaphore(connection);
        await semaphore.WaitAsync(timeout.Token);

        return new SemaphoreReleaser(semaphore);
    }

    public IDisposable GetLock(IDbConnection connection)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        
        var semaphore = GetSemaphore(connection);
        semaphore.Wait(timeout.Token);

        return new SemaphoreReleaser(semaphore);
    }

    SemaphoreSlim GetSemaphore(IDbConnection connection)
    {
        var bucket = GetIntBucket(connection, buckets);
        var semaphore = _semaphores.GetOrAdd(bucket, _ => new SemaphoreSlim(initialCount: 1));
        return semaphore;
    }

    internal static int GetIntBucket(object obj, int bucketCount)
    {
        return (int)((uint)obj.GetHashCode() % bucketCount);
    }

    readonly struct SemaphoreReleaser(SemaphoreSlim semaphore) : IDisposable
    {
        public void Dispose() => semaphore.Release();
    }

    public void Dispose()
    {
        foreach (var semaphore in _semaphores.Values)
        {
            semaphore.Dispose();
        }
    }
}