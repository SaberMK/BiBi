using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Base;
using System;
using System.Collections.Generic;

namespace BB.Memory.Logger
{
    public sealed class LogManager : ILogManager
    {
        private readonly IFileManager _fileManager;
        private readonly string _logFilename;
        private readonly Page _page;
        private readonly object _bufferLock = new object();

        public static readonly int LAST_ENTRY_STORAGE_POSITION = 0;

        private Block _currentBlock;
        private int _currentPosition;

        private int _lsn;

        public LogManager(IFileManager fileManager, string logFilename)
        {
            _fileManager = fileManager;
            _logFilename = logFilename;
            _page = _fileManager.ResolvePage();
            _lsn = -1;

            var logSize = fileManager.Length(logFilename);

            if (logSize == 0)
            {
                AppendNewBlock();
            }
            else
            {
                _currentBlock = new Block(logFilename, fileManager.LastBlockId(logFilename));
                _page.Read(_currentBlock);

                _currentPosition = LastRecordPosition + sizeof(int);
            }
        }

        public void Flush(int lsn)
        {
            if (lsn >= _currentBlock.Id)
            {
                Flush();
            }
        }

        public bool Append(object[] records, out int lsn)
        {
            lsn = 0;

            var totalRecordSize = 0;
            foreach (var entry in records)
            {
                var size = Size(entry);

                if (size == 0)
                    return false;

                totalRecordSize += Size(entry);
            }

            if (totalRecordSize + sizeof(int) >= _fileManager.BlockSize)
                return false;

            lock (_bufferLock)
            {
                if (totalRecordSize + _currentPosition + sizeof(int) >= _fileManager.BlockSize)
                {
                    Flush();
                    AppendNewBlock();
                }

                foreach (var entry in records)
                {
                    Append(entry);
                }

                FinalizeRecord();

                _lsn++; 
                lsn = _lsn;
            }

            return true;
        }

        public IEnumerator<BasicLogRecord> GetEnumerator()
        {
            Flush();
            return new LogEnumerator(_fileManager, _currentBlock);
        }

        private void Append(object entry)
        {
            // Check out performance for it, maybe change to something more native
            // But let it be like this for now

            switch (entry)
            {
                case int intValue:
                    _page.SetInt(_currentPosition, intValue);
                    _currentPosition += sizeof(int);
                    return;

                case byte byteValue:
                    _page.SetByte(_currentPosition, byteValue);
                    _currentPosition += sizeof(byte);
                    return;

                case bool boolValue:
                    _page.SetBool(_currentPosition, boolValue);
                    _currentPosition += sizeof(bool);
                    return;

                case byte[] blobValue:
                    _page.SetBlob(_currentPosition, blobValue);
                    _currentPosition += sizeof(int) + blobValue.Length;
                    return;

                case string stringValue:
                    _page.SetString(_currentPosition, stringValue);
                    _currentPosition += sizeof(int) + stringValue.Length * 1 /* size in bytes */;
                    return;

                case DateTime dateTimeValue:
                    _page.SetDate(_currentPosition, dateTimeValue);
                    _currentPosition += sizeof(long);
                    return;
            }
        }

        private int Size(object entry)
        {
            // Check out performance for it, maybe change to something more native
            // But let it be like this for now

            switch (entry)
            {
                case int _:
                    return sizeof(int);

                case byte _:
                    return sizeof(byte);

                case bool _:
                    return sizeof(bool);

                case byte[] blobValue:
                    return sizeof(int) + blobValue.Length;

                case string stringValue:
                    return sizeof(int) + stringValue.Length * 1 /* size in bytes */;

                case DateTime _:
                    return sizeof(long);

                default:
                    return 0;
            }
        }

        private void Flush()
        {
            _page.Write(_currentBlock);
        }

        private void AppendNewBlock()
        {
            _ = _page.Append(_logFilename, out _currentBlock);
            _ = _page.Read(_currentBlock);
            _currentPosition = sizeof(int);
        }

        private void FinalizeRecord()
        {
            _page.SetInt(_currentPosition, LastRecordPosition);
            LastRecordPosition = _currentPosition;
            _currentPosition += sizeof(int);
        }

        private int LastRecordPosition
        {
            get
            {
                _ = _page.GetInt(LAST_ENTRY_STORAGE_POSITION, out var value);
                return value;
            }
            set
            {
                _ = _page.SetInt(LAST_ENTRY_STORAGE_POSITION, value);
            }
        }
    }
}
