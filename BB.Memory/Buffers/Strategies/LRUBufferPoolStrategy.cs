using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Base;
using System.Collections.Generic;
using DateTime = System.DateTime;

namespace BB.Memory.Buffers.Strategies
{
    public class LRUBufferPoolStrategy : IBufferPoolStrategy
    {
        private readonly SortedList<long, Buffer> _bufferPool;
        private readonly int _totalBuffers;
        private int _available;

        private readonly object _poolLock = new object();
        private readonly ILogManager _logManager;
        private readonly IFileManager _fileManager;
        public LRUBufferPoolStrategy(ILogManager logManager, IFileManager fileManager, int totalBuffers)
        {
            _fileManager = fileManager;
            _logManager = logManager;

            _available = totalBuffers;
            _totalBuffers = totalBuffers;

            _bufferPool = new SortedList<long, Buffer>(totalBuffers);
        }

        public void FlushAll(int transactionNumber)
        {
            lock (_poolLock)
            {
                foreach (var buffer in _bufferPool.Values)
                {
                    if (buffer.IsModifiedBy(transactionNumber))
                        buffer.Flush();
                }
            }
        }

        public Buffer Pin(Block block)
        {
            lock (_poolLock)
            {
                Buffer buffer = FindExistingBuffer(block);

                if (buffer == null)
                {
                    if (_bufferPool.Count < _totalBuffers)
                    {
                        buffer = AddNewBuffer();
                        buffer.AssignToBlock(block);
                    }
                    else
                    {
                        return null;
                    }
                }

                if (!buffer.IsPinned)
                    _available--;

                buffer.Pin();
                return buffer;
            }
        }

        public Buffer PinNew(string filename, IPageFormatter pageFormatter)
        {
            lock (_poolLock)
            {
                Buffer buffer = null;
                if (_bufferPool.Count < _totalBuffers)
                {
                    buffer = AddNewBuffer();
                    buffer.AssignToNew(filename, pageFormatter);
                }
                else
                {
                    return null;
                }

                if (!buffer.IsPinned)
                    _available--;

                buffer.Pin();
                return buffer;
            }
        }

        public void Unpin(Buffer buffer)
        {
            // TODO: Read about it a bit more, and maybe it is not a good idea to lock on buffer>
            // TODO: Think - should buffer be a structure?

            lock (_poolLock)
            {
                buffer.Unpin();
                if (!buffer.IsPinned)
                {
                    _available++;

                    // There are still multiple ways to optimize it, 
                    // I.e. do not recreate an object every time

                    var index = _bufferPool.IndexOfValue(buffer);
                    _bufferPool.RemoveAt(index);
                }
            }
        }

        private Buffer AddNewBuffer()
        {
            var buffer = new Buffer(_logManager, _fileManager);

            _bufferPool.Add(DateTime.UtcNow.Ticks, buffer);

            return buffer;
        }

        private Buffer FindExistingBuffer(Block block)
        {
            var collection = _bufferPool.Values;
            for (int i = 0; i < collection.Count; ++i)
            {
                if (block == collection[i].Block)
                    return collection[i];
            }

            return null;
        }

        public int Available => _available;
    }
}
