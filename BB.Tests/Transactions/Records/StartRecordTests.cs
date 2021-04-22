using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Transactions.Abstract;
using BB.Transactions.Records;
using NUnit.Framework;
using System;
using System.IO;

namespace BB.Tests.Transactions.Records
{
    public class StartRecordTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private IBufferManager _bufferManager;
        private LogRecord _logRecord;
        private string _logFileName;

        [SetUp]
        public void Setup()
        {
            _fileManager = new FileManager("temp", "DBs", 100);
            _logFileName = RandomFilename;
            _logManager = new LogManager(_fileManager, _logFileName);
            var _strategy = new LRUBufferPoolStrategy(_logManager, _fileManager, 10);
            _bufferManager = new BufferManager(_fileManager, _logManager, _strategy, null, null);
        }

        [Test]
        public void CanCreateStartRecord()
        {
            Assert.DoesNotThrow(() =>
            {
                _logRecord = new StartRecord(_logManager, _bufferManager, 1);
            });
        }

        [Test]
        public void CanWriteStartRecordToLog()
        {
            _logRecord = new StartRecord(_logManager, _bufferManager, 1);
            var lsn = _logRecord.WriteToLog();

            _logManager.Flush(lsn);
            var page = _fileManager.ResolvePage();
            _ = page.Read(new Block(_logFileName, 0));

            _ = page.GetInt(0, out var recordLength);
            _ = page.GetInt(sizeof(int), out var recordType);
            _ = page.GetInt(sizeof(int) * 2, out var transactionId);

            Assert.AreEqual(0, lsn);
            Assert.AreEqual(sizeof(int) + sizeof(LogRecordType) + sizeof(int), recordLength);
            Assert.AreEqual((int)LogRecordType.Start, recordType);
            Assert.AreEqual(1, transactionId);
        }

        [Test]
        public void CanWriteMultipleStartRecordsToLog()
        {
            _logRecord = new StartRecord(_logManager, _bufferManager, 1);
            var lsn = _logRecord.WriteToLog();
            var lsn2 = new StartRecord(_logManager, _bufferManager, 2).WriteToLog();

            _logManager.Flush(lsn2);
            var page = _fileManager.ResolvePage();
            _ = page.Read(new Block(_logFileName, 0));

            _ = page.GetInt(0, out var nextResPosition);
            _ = page.GetInt(sizeof(int), out var recordType1);
            _ = page.GetInt(sizeof(int) * 2, out var transactionId1);


            _ = page.GetInt(12 + 0, out var recordLength2);
            _ = page.GetInt(12 + sizeof(int), out var recordType2);
            _ = page.GetInt(12 + sizeof(int) * 2, out var transactionId2);

            Assert.AreEqual(0, lsn);
            Assert.AreEqual(1, lsn2);

            Assert.AreEqual((sizeof(int) + sizeof(LogRecordType) + sizeof(int)) * 2, nextResPosition);
            Assert.AreEqual((int)LogRecordType.Start, recordType1);
            Assert.AreEqual(1, transactionId1);

            Assert.AreEqual(0, recordLength2);
            Assert.AreEqual((int)LogRecordType.Start, recordType2);
            Assert.AreEqual(2, transactionId2);
        }

        [Test]
        public void CanReadBackwardsFromLog()
        {
            _logRecord = new StartRecord(_logManager, _bufferManager, 1);
            var lsn = _logRecord.WriteToLog();

            _logManager.Flush(lsn);

            var enumerator = _logManager.GetEnumerator();
            var basicLogRecord = enumerator.Current;

            var logRecord = new StartRecord(_logManager, _bufferManager, basicLogRecord);

            Assert.AreEqual(LogRecordType.Start, logRecord.Type);
            Assert.AreEqual(1, logRecord.TransactionNumber);
        }


        [Test]
        public void CanUndoRecord()
        {
            Assert.DoesNotThrow(() =>
            {
                _logRecord = new StartRecord(_logManager, _bufferManager, 5);
                _logRecord.Undo();
            });
        }

        [Test]
        public void ToStringReturnsMeaningfullInfo()
        {
            _logRecord = new StartRecord(_logManager, _bufferManager, 5);

            var result = _logRecord
                .ToString()
                .ToUpper();

            Assert.IsTrue(result.Contains("START"));
            Assert.IsTrue(result.Contains("5"));
        }

        [OneTimeTearDown]
        public void ClearDirectory()
        {
            Directory.Delete("DBs", true);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
