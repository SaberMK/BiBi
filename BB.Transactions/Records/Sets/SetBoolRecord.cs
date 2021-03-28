using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Transactions.Abstract;

namespace BB.Transactions.Records
{
    // ref struct?
    public class SetBoolRecord : LogRecord, IDataLogRecord<bool>
    {
        private readonly int _offset;
        private bool _value;
        private Block _block;

        public SetBoolRecord(
            ILogManager logManager,
            IBufferManager bufferManager,
            int transactionNumber,
            Block block,
            int offset,
            bool value)
            : base(logManager, bufferManager, LogRecordType.SetBool)
        {
            _transactionNumber = transactionNumber;
            _block = block;
            _offset = offset;
            _value = value;
        }

        public SetBoolRecord(
            ILogManager logManager,
            IBufferManager bufferManager,
            BasicLogRecord record)
            : base(logManager, bufferManager, LogRecordType.SetBool)
        {
            _ = record.NextInt(out _transactionNumber);
            _ = record.NextString(out var filename);
            _ = record.NextInt(out var blockNumber);

            _block = new Block(filename, blockNumber);
            _ = record.NextInt(out _offset);
            _ = record.NextBool(out _value);
        }

        public override int WriteToLog()
        {
            var record = new object[]
            {
                (int)LogRecordType.SetBool,
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
            buffer.SetBool(_offset, _value, _transactionNumber, -1);
            _bufferManager.Unpin(buffer);
        }

        public override string ToString()
            => $"<SETBOOL {_transactionNumber} {_block.Filename} {_block.Id} {_offset} {_value}>";

        public int Offset => _offset;
        public bool Value => _value;
        public Block Block => _block;
    }
}
