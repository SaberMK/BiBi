using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace BB.Memory.Buffers.Strategies
{
    public class NaiveBufferStrategy : IBufferStrategy
    {
        private readonly IDirectoryManager _directoryManager;
        private readonly ILogManager _logManager;
        private readonly Buffer[] _bufferPool;

        private int _availableBuffers;

        public NaiveBufferStrategy(
            IDirectoryManager directoryManager, 
            ILogManager logManager,
            int buffersAmount)
        {
            _directoryManager = directoryManager;
            _logManager = logManager;
            _availableBuffers = buffersAmount;

            _bufferPool = new Buffer[buffersAmount];

            for (int i = 0; i < buffersAmount; ++i)
                _bufferPool[i] = new Buffer(logManager);
        }

        // TODO move to NaiveStrategy
        // TODO Monitor.... <- use this shit
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Buffer Pin(Block block)
        {
            var buffer = FindExistingBuffer(block);

            if (buffer == null)
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

        public void Unpin(Buffer buffer)
        {
            if (!buffer.IsPinned)
                return;

            buffer.Unpin();
            if (!buffer.IsPinned)
            {
                Interlocked.Increment(ref _availableBuffers);
            }
        }

        private Buffer FindExistingBuffer(Block block)
        {
            foreach (var buffer in _bufferPool)
            {
                if (buffer.Block == block)
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

        public IEnumerable<Buffer> Buffers => _bufferPool;
        public int Available => _availableBuffers;
        public StrategyType Type => StrategyType.Naive;
    }
}
