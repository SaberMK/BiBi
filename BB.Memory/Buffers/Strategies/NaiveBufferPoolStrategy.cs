using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Base;
using System.Collections.Generic;
using System.Text;

namespace BB.Memory.Buffers.Strategies
{
    public class NaiveBufferPoolStrategy : IBufferPoolStrategy
    {
        private readonly Buffer[] _bufferPool;
        private int _available;

        private readonly object _flushLock = new object();
        private readonly object _poolLock = new object();

        public NaiveBufferPoolStrategy(ILogManager logManager, IFileManager fileManager, int totalBuffers)
        {
            _available = totalBuffers;

            _bufferPool = new Buffer[totalBuffers];

            for(int i =0;i<totalBuffers;++i)
            {
                _bufferPool[i] = new Buffer(logManager, fileManager);
            }
        }

        public void FlushAll(int transactionNumber)
        {
            lock (_flushLock)
            {
                for (int i = 0; i < _bufferPool.Length; ++i)
                {
                    if (_bufferPool[i].IsModifiedBy(transactionNumber))
                        _bufferPool[i].Flush();
                }
            }
        }

        public Buffer Pin(Block block)
        {
            // Check out, would it be possible to be on a lock as low amount of time as possible?

            lock (_poolLock)
            {
                var buffer = FindExistingBuffer(block);
                if (buffer == null)
                {
                    buffer = ChooseUnpinnedBuffer();

                    if (buffer == null)
                        return null;

                    buffer.AssignToBlock(block);
                }

                if (!buffer.IsPinned)
                    _available--;

                buffer.Pin();
                return buffer;
            }
        }

        public Buffer PinNew(string filename, IPageFormatter pageFormatter)
        {
            // Check out, would it be possible to be on a lock as low amount of time as possible?

            lock (_poolLock)
            {
                var buffer = ChooseUnpinnedBuffer();
                if (buffer == null)
                    return null;

                buffer.AssignToNew(filename, pageFormatter);
                _available--;
                buffer.Pin();
                return buffer;
            }
        }

        public void Unpin(Buffer buffer)
        {
            // TODO: Read about it a bit more, and maybe it is not a good idea to lock on buffer>
            // TODO: Think - should buffer be a structure?

            lock (buffer)
            {
                buffer.Unpin();
                if (!buffer.IsPinned)
                    _available++;
            }
        }

        public int Available => _available;
        

        private Buffer FindExistingBuffer(Block block)
        {
            for(int i = 0; i < _bufferPool.Length; ++i)
            {
                if (block == _bufferPool[i].Block)
                    return _bufferPool[i];
            }

            return null;
        }

        private Buffer ChooseUnpinnedBuffer()
        {
            for(int i = 0; i < _bufferPool.Length; ++i)
            {
                if (!_bufferPool[i].IsPinned)
                    return _bufferPool[i];
            }

            return null;
        }
    }
}
