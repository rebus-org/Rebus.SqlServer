using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Rebus.SqlServer.DataBus
{
    /// <summary>
    /// Wraps a stream and an action, calling the action when the stream is disposed
    /// </summary>
    class StreamWrapper : Stream
    {
        readonly Stream _innerStream;
        readonly IDisposable[] _disposables;

        public StreamWrapper(Stream innerStream, IEnumerable<IDisposable> disposables)
        {
            _innerStream = innerStream ?? throw new ArgumentNullException(nameof(innerStream));
            if (disposables == null) throw new ArgumentNullException(nameof(disposables));
            _disposables = disposables.ToArray();
        }

        public override void Flush()
        {
            _innerStream.Flush();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _innerStream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _innerStream.SetLength(value);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return _innerStream.Read(buffer, offset, count);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _innerStream.Write(buffer, offset, count);
        }

        public override bool CanRead => _innerStream.CanRead;
        public override bool CanSeek => _innerStream.CanSeek;
        public override bool CanWrite => _innerStream.CanWrite;
        public override long Length => _innerStream.Length;

        public override long Position
        {
            get { return _innerStream.Position; }
            set { _innerStream.Position = value; }
        }

        protected override void Dispose(bool disposing)
        {
            foreach (var disposable in _disposables)
            {
                disposable.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
