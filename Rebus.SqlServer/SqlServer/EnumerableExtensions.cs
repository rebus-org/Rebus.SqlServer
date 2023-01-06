using System;
using System.Collections.Generic;
using System.Linq;

namespace Rebus.SqlServer;

static class EnumerableExtensions
{
    public static IEnumerable<IReadOnlyList<T>> Batch<T>(this IEnumerable<T> items, int batchSize)
    {
        if (items == null) throw new ArgumentNullException(nameof(items));
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize), "Please use a batch size >= 1");

        List<T> NewList() => new(capacity: batchSize);

        var list = NewList();

        foreach (var item in items)
        {
            list.Add(item);
            if (list.Count < batchSize) continue;

            yield return list;
            list = NewList();
        }

        if (list.Any())
        {
            yield return list;
        }
    }
}