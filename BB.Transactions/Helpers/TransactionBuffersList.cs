using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Base;
using System.Collections.Generic;

namespace BB.Transactions.Helpers
{
    public class TransactionBuffersList
    {
        private readonly IBufferManager _bufferManager;
        private readonly Dictionary<Block, Buffer> _buffers;
        private readonly List<Block> _pins;

        public TransactionBuffersList(IBufferManager bufferManager)
        {
            _buffers = new Dictionary<Block, Buffer>();
            _pins = new List<Block>();
            _bufferManager = bufferManager;
        }

        public Buffer GetBuffer(Block block)
        {
            var hasBuffer = _buffers.TryGetValue(block, out var buffer);
            return hasBuffer ? buffer : null;
        }

        public void Pin(Block block)
        {
            var buffer = _bufferManager.Pin(block);

            _buffers.Add(block, buffer);
            _pins.Add(block);
        }

        public Block PinNew(string filename, IPageFormatter pageFormatter)
        {
            var buffer = _bufferManager.PinNew(filename, pageFormatter);
            var block = buffer.Block;

            _buffers.Add(block, buffer);
            _pins.Add(block);

            return block;
        }

        public void Unpin(Block block)
        {
            if(_buffers.TryGetValue(block, out var buffer))
            {
                _bufferManager.Unpin(buffer);
                _pins.Remove(block);

                // Don't think that would need this check. 
                // Anyway, let it be for now, code coverage would show who is right
                if (!_pins.Contains(block))
                    _buffers.Remove(block);
            }
        }

        public void UnpinAll()
        {
            for(int i = 0; i < _pins.Count; ++i)
            {
                var buffer = _buffers[_pins[i]];
                _bufferManager.Unpin(buffer);
            }

            _buffers.Clear();
            _pins.Clear();
        }
    }
}
