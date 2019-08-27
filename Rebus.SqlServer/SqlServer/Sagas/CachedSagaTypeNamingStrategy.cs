using System;
using System.Collections.Concurrent;

namespace Rebus.SqlServer.Sagas
{
    /// <summary>
    /// Decorator for <seealso cref="ISagaTypeNamingStrategy"/> that caches the results in-case calculations are expensive
    /// </summary>
    public class CachedSagaTypeNamingStrategy : ISagaTypeNamingStrategy
    {
        private readonly ConcurrentDictionary<Type, string> _sagaTypeCache = new ConcurrentDictionary<Type, string>();
        private readonly ISagaTypeNamingStrategy _innerSagaTypeNamingStrategy;

        /// <summary>
        /// Constructs a new instance which will defer actual naming to <paramref name="innerSagaTypeNamingStrategy"/>
        /// </summary>
        public CachedSagaTypeNamingStrategy(ISagaTypeNamingStrategy innerSagaTypeNamingStrategy)
        {
            _innerSagaTypeNamingStrategy = innerSagaTypeNamingStrategy;
        }

        /// <inheritdoc />
        public string GetSagaTypeName(Type sagaDataType, int maximumLength)
        {
            return _sagaTypeCache.GetOrAdd(sagaDataType, t => _innerSagaTypeNamingStrategy.GetSagaTypeName(t, maximumLength));
        }
    }
}
