using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Transactions.Abstract;
using System;

namespace BB.Transactions.Records
{
    // ref struct?
    public class SetDateRecord : LogRecord, IDataLogRecord<DateTime>
    {
        private readonly int _offset;
        private DateTime _value;
        private Block _block;

        public SetDateRecord(
            ILogManager logManager,
            IBufferManager bufferManager,
            int transactionNumber,
            Block block,
            int offset,
            DateTime value)
            : base(logManager, bufferManager, LogRecordType.SetDate)
        {
            _transactionNumber = transactionNumber;
            _block = block;
            _offset = offset;
            _value = value;
        }

        public SetDateRecord(
            ILogManager logManager,
            IBufferManager bufferManager,
            BasicLogRecord record)
            : base(logManager, bufferManager, LogRecordType.SetDate)
        {
            _ = record.NextInt(out _transactionNumber);
            _ = record.NextString(out var filename);
            _ = record.NextInt(out var blockNumber);

            _block = new Block(filename, blockNumber);
            _ = record.NextInt(out _offset);
            _ = record.NextDate(out _value);
        }

        public override int WriteToLog()
        {
            var record = new object[]
            {
                (int)LogRecordType.SetDate,
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

            // TODO passing -1 to the log value is not that accurate
            // Should I rethink it or not?
            buffer.SetDate(_offset, _value, _transactionNumber, -1);
            _bufferManager.Unpin(buffer);
        }

        public override string ToString()
            => $"<SETDATE {_transactionNumber} {_block.Filename} {_block.Id} {_offset} {_value.ToShortDateString()}>";

        public int Offset => _offset;
        public DateTime Value => _value;
        public Block Block => _block;
    }
}
