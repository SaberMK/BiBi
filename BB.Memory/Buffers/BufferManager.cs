using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers.Strategies;
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
        private readonly IBufferStrategy _strategy;

        private readonly long _maxTimeForBufferAwaiting; // 10 seconds for deadlock awaiting
        private static readonly int _tickAwaiting = (int)TimeSpan.FromMilliseconds(100).Ticks;

        public BufferManager(
            IDirectoryManager directoryManager, 
            ILogManager logManager,
            StrategyType strategy = StrategyType.Naive,
            int buffersAmount = 50, 
            TimeSpan? maxTimeForBufferAwaiting = null)
        {
            _maxTimeForBufferAwaiting = maxTimeForBufferAwaiting.HasValue 
                ? maxTimeForBufferAwaiting.Value.Ticks
                : TimeSpan.FromSeconds(10).Ticks;

            _strategy = SelectStrategy(strategy, directoryManager, logManager, buffersAmount);
        }

        private IBufferStrategy SelectStrategy(
            StrategyType strategyType, 
            IDirectoryManager dirManager, 
            ILogManager logManager, 
            int buffersAmount)
        {
            switch (strategyType)
            {
                case StrategyType.Naive:
                    return new NaiveBufferStrategy(dirManager, logManager, buffersAmount);

                default:
                    // ???
                    return null;
            }
        }

        // TODO move to NaiveStrategy
        // TODO Monitor.... <- use this shit
        public Buffer Pin(Block block)
        {
            var ticks = DateTime.UtcNow.Ticks;

            var buffer = _strategy.Pin(block);

            while(buffer == null && !WaitingForTooLong(ticks))
            {
                Thread.SpinWait(_tickAwaiting);
                buffer = _strategy.Pin(block);
            }

            if (buffer == null)
                throw new BufferAbortionException();

            return buffer;
        }

        public void Unpin(Buffer buffer)
        {
            _strategy.Unpin(buffer);
        }

        public void FlushAll(int transactionNumber)
        {
            foreach(var buffer in _strategy.Buffers)
            {
                if (buffer.ModyfyingTransaction == transactionNumber)
                    buffer.Flush();
            }
        }

        private bool WaitingForTooLong(long startTime)
        {
            return startTime + _maxTimeForBufferAwaiting < DateTime.UtcNow.Ticks;
        }

        public int Available => _strategy.Available;

        public void Dispose()
        {
            // clear buffers?
        }
    }
}
