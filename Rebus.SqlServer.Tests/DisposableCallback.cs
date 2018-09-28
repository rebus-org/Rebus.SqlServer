using System;

namespace Rebus.SqlServer.Tests
{
    class DisposableCallback : IDisposable
    {
        readonly Action _disposeAction;

        public DisposableCallback(Action disposeAction) => _disposeAction = disposeAction;

        public void Dispose() => _disposeAction();
    }
}