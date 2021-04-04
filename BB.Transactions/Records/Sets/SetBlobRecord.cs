using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Transactions.Abstract;

namespace BB.Transactions.Records
{
    // ref struct?
    public class SetBlobRecord : LogRecord, IDataLogRecord<byte[]>
    {
        private readonly int _offset;
        private byte[] _value;
        private Block _block;

        public SetBlobRecord(
            ILogManager logManager,
            IBufferManager bufferManager,
            int transactionNumber,
            Block block,
            int offset,
            byte[] value)
            : base(logManager, bufferManager, LogRecordType.SetBlob)
        {
            _transactionNumber = transactionNumber;
            _block = block;
            _offset = offset;
            _value = value;
        }

        public SetBlobRecord(
            ILogManager logManager,
            IBufferManager bufferManager,
            BasicLogRecord record,
            bool needOffset = true)
            : base(logManager, bufferManager, LogRecordType.SetBlob)
        {
            if (needOffset)
            {
                _ = record.NextInt(out var _);
            }

            _ = record.NextInt(out _transactionNumber);
            _ = record.NextString(out var filename);
            _ = record.NextInt(out var blockNumber);

            _block = new Block(filename, blockNumber);
            _ = record.NextInt(out _offset);
            _ = record.NextBlob(out _value);
        }

        public override int WriteToLog()
        {
            var record = new object[]
            {
                (int)LogRecordType.SetBlob,
                _transactionNumber,
                _block.Filename,
                _block.Id,
                _offset,
                _value
            };

            _ = _logManager.Append(record, out var lsn);

            return lsn;
        }

        public override void Undo()
        {
            var buffer = _bufferManager.Pin(_block);
            buffer.SetBlob(_offset, _value, _transactionNumber, -1);
            _bufferManager.Unpin(buffer);
        }

        public override string ToString()
            => $"<SETBLOB {_transactionNumber} {_block.Filename} {_block.Id} {_offset} [{string.Join(", ", _value)}]>";

        public int Offset => _offset;
        public byte[] Value => _value;
        public Block Block => _block;
    }
}
