using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Transactions.Abstract;
using BB.Transactions.Records;
using System.Collections;
using System.Collections.Generic;

namespace BB.Transactions.Recovery
{
    public class LogRecordEnumerator : IEnumerator<LogRecord>
    {
        private readonly IEnumerator<BasicLogRecord> _enumerator;
        private readonly ILogManager _logManager;
        private readonly IBufferManager _bufferManager;
        public LogRecordEnumerator(ILogManager logManager, IBufferManager bufferManager)
        {
            _bufferManager = bufferManager;
            _logManager = logManager;

            _enumerator = logManager.GetEnumerator();
        }

        public LogRecord Current
        {
            get
            {
                var record = _enumerator.Current;
                _ = record.NextInt(out var typeNumber);
                var type = (LogRecordType)typeNumber;

                switch (type)
                {
                    case LogRecordType.Checkpoint:
                        return new CheckpointRecord(_logManager, _bufferManager, record);

                    case LogRecordType.Start:
                        return new StartRecord(_logManager, _bufferManager, record);

                    case LogRecordType.Commit:
                        return new CommitRecord(_logManager, _bufferManager, record);

                    case LogRecordType.Rollback:
                        return new RollbackRecord(_logManager, _bufferManager, record);

                    case LogRecordType.SetInt:
                        return new SetIntRecord(_logManager, _bufferManager, record);

                    case LogRecordType.SetByte:
                        return new SetByteRecord(_logManager, _bufferManager, record);

                    case LogRecordType.SetBool:
                        return new SetBoolRecord(_logManager, _bufferManager, record);

                    case LogRecordType.SetBlob:
                        return new SetBlobRecord(_logManager, _bufferManager, record);

                    case LogRecordType.SetString:
                        return new SetStringRecord(_logManager, _bufferManager, record);

                    case LogRecordType.SetDate:
                        return new SetDateRecord(_logManager, _bufferManager, record);

                    default:
                        return null;
                }
            }
        }

        object IEnumerator.Current => this;

        public bool MoveNext()
        {
            return _enumerator.MoveNext();
        }

        public void Reset()
        {
            _enumerator.Reset();
        }

        public void Dispose()
        {
            _enumerator.Dispose();
        }
    }
}
