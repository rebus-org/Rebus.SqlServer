using System;
using System.IO;

namespace Rebus.SqlServer.DataBus
{
    class StreamWrapper : Stream
    {
        readonly Stream _innerStream;
        readonly Action _disposeAction;

        public StreamWrapper(Stream innerStream,  Action disposeAction)
        {
            if (innerStream == null) throw new ArgumentNullException(nameof(innerStream));
            _innerStream = innerStream;
            _disposeAction = disposeAction;
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
            _disposeAction();
            base.Dispose(disposing);
        }
    }

}