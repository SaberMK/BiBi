using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Memory.Exceptions;
using System.Threading;
using DateTime = System.DateTime;
using TimeSpan = System.TimeSpan;

namespace BB.Memory.Buffers
{
    public sealed class BufferManager : IBufferManager
    {
        private readonly long _maxWaitingTime;
        private readonly int _tickWaitingTime;
        private readonly IFileManager _fileManager;
        private readonly ILogManager _logManager;
        private readonly IBufferPoolStrategy _poolStrategy;
        private readonly object _bufferGatheringLock = new object();


        public BufferManager(
            IFileManager fileManager,
            ILogManager logManager,
            IBufferPoolStrategy poolStrategy,
            TimeSpan? maxWaitingTime = null,
            TimeSpan? tickWaitingTime = null)
        {
            _fileManager = fileManager;
            _logManager = logManager;
            _poolStrategy = poolStrategy;
            _maxWaitingTime = maxWaitingTime?.Ticks ?? TimeSpan.FromSeconds(5).Ticks;
            _tickWaitingTime = (int)(tickWaitingTime?.TotalMilliseconds ?? 200);
        }



        public Buffer Pin(Block block)
        {
            //try
            //{
            long timestamp = DateTime.UtcNow.Ticks;
            Buffer buffer = null;

            lock (_bufferGatheringLock)
            {
                buffer = _poolStrategy.Pin(block);
            }

            while (buffer == null && !WaitingForTooLong(timestamp))
            {
                Thread.Sleep(_tickWaitingTime);

                lock (_bufferGatheringLock)
                {
                    buffer = _poolStrategy.Pin(block);
                }
            }

            if (buffer == null)
                throw new BufferBusyException();

            return buffer;
            //}
            //catch(ThreadAbortException)
            //{
            //    throw new BufferBusyException();
            //}
        }

        public Buffer PinNew(string filename, IPageFormatter pageFormatter)
        {
            //try
            //{
            long timestamp = DateTime.UtcNow.Ticks;

            Buffer buffer = null;

            lock (_bufferGatheringLock)
            {
                buffer = _poolStrategy.PinNew(filename, pageFormatter);
            }

            while (buffer == null && !WaitingForTooLong(timestamp))
            {
                Thread.Sleep(_tickWaitingTime);

                lock (_bufferGatheringLock)
                {
                    buffer = _poolStrategy.PinNew(filename, pageFormatter);
                }
            }

            if (buffer == null)
                throw new BufferBusyException();

            return buffer;
            //}
            //catch (ThreadAbortException)
            //{
            //    throw new BufferBusyException();
            //}
        }

        public void Unpin(Buffer buffer)
        {
            // Maybe Monitor.PulseAll is not a good think, but at lease it would wake up threads.
            lock (_bufferGatheringLock)
            {
                _poolStrategy.Unpin(buffer);
                if (!buffer.IsPinned)
                    Monitor.PulseAll(_bufferGatheringLock);
            }
        }

        public void FlushAll(int transactionNumber)
        {
            _poolStrategy.FlushAll(transactionNumber);
        }

        public int Available => _poolStrategy.Available;

        private bool WaitingForTooLong(long timestamp)
        {
            return timestamp + _maxWaitingTime < DateTime.UtcNow.Ticks;
        }
    }
}
