using System;
using System.Collections.Generic;
using System.Linq;

namespace Rebus.Extensions;

static class EnumerableExtensions
{
    public static IEnumerable<IReadOnlyList<T>> Batch<T>(this IEnumerable<T> items, int batchSize)
    {
        if (items == null) throw new ArgumentNullException(nameof(items));
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize, "Please pass a batch size greater than or equal to 1");

        var list = new List<T>(capacity: batchSize);

        foreach (var item in items)
        {
            list.Add(item);

            if (list.Count < batchSize) continue;

            yield return list;

            list = new List<T>(capacity: batchSize);
        }

        if (list.Any())
        {
            yield return list;
        }
    }
}