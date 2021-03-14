using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Exceptions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BB.Memory.Buffers
{
    public class BufferManager : IBufferManager
    {
        private readonly IDirectoryManager _directoryManager;
        private readonly ILogManager _logManager;
        private readonly int _buffersAmount;
        private readonly Buffer[] _bufferPool;

        private readonly long _maxTimeForBufferAwaiting; // 10 seconds for deadlock awaiting
        private static readonly int _tickAwaiting = (int)TimeSpan.FromMilliseconds(100).Ticks;

        private int _availableBuffers;

        public BufferManager(
            IDirectoryManager directoryManager, 
            ILogManager logManager, 
            int buffersAmount = 50, 
            TimeSpan? maxTimeForBufferAwaiting = null)
        {
            _maxTimeForBufferAwaiting = maxTimeForBufferAwaiting.HasValue 
                ? maxTimeForBufferAwaiting.Value.Ticks
                : TimeSpan.FromSeconds(10).Ticks;

            _logManager = logManager;
            _directoryManager = directoryManager;
            _buffersAmount = buffersAmount;
            _bufferPool = new Buffer[buffersAmount];
            _availableBuffers = buffersAmount;

            for (int i = 0; i < buffersAmount; ++i)
                _bufferPool[i] = new Buffer(logManager);
        }

        // TODO move to NaiveStrategy
        // TODO Monitor.... <- use this shit
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Buffer Pin(Block block)
        {
            var timestamp = DateTime.UtcNow.Ticks;

            var buffer = TryToPin(block);

            while (buffer == null && !WaitingForTooLong(timestamp))
            {
                Thread.SpinWait(_tickAwaiting);
                buffer = TryToPin(block);
            }

            if (buffer == null)
                throw new BufferAbortionException();

            return buffer;
        }

        public void Unpin(Buffer buffer)
        {
            buffer.Unpin();
            if(!buffer.IsPinned)
            {
                Interlocked.Increment(ref _availableBuffers);
            }
        }

        public void FlushAll(int transactionNumber)
        {
            foreach(var buffer in _bufferPool)
            {
                if (buffer.ModyfyingTransaction == transactionNumber)
                    buffer.Flush();
            }
        }

        private bool WaitingForTooLong(long startTime)
        {
            return startTime + _maxTimeForBufferAwaiting < DateTime.UtcNow.Ticks;
        }

        private Buffer TryToPin(Block block)
        {
            var buffer = FindExistingBuffer(block);

            if(buffer == null)
            {
                buffer = ChooseUnpinnedBuffer();
                if (buffer == null)
                    return null;

                buffer.AssignToBlock(block.Id, _directoryManager.GetManager(block.Filename));
            }

            if (!buffer.IsPinned)
                Interlocked.Decrement(ref _availableBuffers);

            buffer.Pin();
            return buffer;
        }

        private Buffer FindExistingBuffer(Block block)
        {
            foreach(var buffer in _bufferPool)
            {
                if(buffer.Block == block)
                    return buffer;
            }

            return null;
        }

        private Buffer ChooseUnpinnedBuffer()
        {
            // TODO change to for cycle in future
            foreach (var buffer in _bufferPool)
            {
                if (!buffer.IsPinned)
                    return buffer;
            }

            return null;
        }

        public int Available => _availableBuffers;

        public void Dispose()
        {
            // clear buffers?
        }
    }
}
