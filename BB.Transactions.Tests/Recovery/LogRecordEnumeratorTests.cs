using BB.IO;
using BB.IO.Abstract;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Transactions.Abstract;
using BB.Transactions.Recovery;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace BB.Transactions.Tests.Recovery
{
    public class LogRecordEnumeratorTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private IEnumerator<LogRecord> _enumerator;
        private IRecoveryManager _recoveryManager;
        private IBufferManager _bufferManager;

        [SetUp]
        public void Setup()
        {
            _fileManager = new FileManager("temp", "DBs", 100);
            _logManager = new LogManager(_fileManager, RandomFilename);
            _bufferManager = new BufferManager(_fileManager, _logManager, new NaiveBufferPoolStrategy(_logManager, _fileManager, 5));
        }

        [Test]
        public void CanCreateLogEnumerator()
        {
            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 5);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
