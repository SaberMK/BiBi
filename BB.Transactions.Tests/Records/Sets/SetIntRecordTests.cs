using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Transactions.Abstract;
using BB.Transactions.Records;
using BB.Transactions.Recovery;
using NUnit.Framework;
using System;
using System.IO;

namespace BB.Transactions.Tests.Records.Sets
{
    public class SetIntRecordTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private IBufferManager _bufferManager;
        private LogRecord _logRecord;
        private string _logFileName;
        private Block _putToBlock;

        [SetUp]
        public void Setup()
        {
            _fileManager = new FileManager("temp", "DBs", 300);
            _logFileName = RandomFilename;
            _putToBlock = new Block(RandomFilename, 0);
            _logManager = new LogManager(_fileManager, _logFileName);
            var _strategy = new NaiveBufferPoolStrategy(_logManager, _fileManager, 10);
            _bufferManager = new BufferManager(_fileManager, _logManager, _strategy, null, null);
        }

        [Test]
        public void CanCreateSetIntRecord()
        {
            Assert.DoesNotThrow(() =>
            {
                _logRecord = new SetIntRecord(
                    _logManager,
                    _bufferManager,
                    1,
                    _putToBlock,
                    0,
                    123);
            });
        }

        [Test]
        public void CanWriteSetIntRecordToLog()
        {
            _logRecord = new SetIntRecord(
                       _logManager,
                       _bufferManager,
                       3,
                       _putToBlock,
                       4,
                       123);
            var lsn = _logRecord.WriteToLog();

            _logManager.Flush(lsn);

            var enumerator = _logManager.GetEnumerator();
            var record = enumerator.Current;
            var currentRecord = new SetIntRecord(_logManager, _bufferManager, record);

            Assert.IsNotNull(currentRecord);
            Assert.AreEqual(LogRecordType.SetInt, currentRecord.Type);
            Assert.AreEqual(3, currentRecord.TransactionNumber);
            Assert.AreEqual(123, currentRecord.Value);
            Assert.AreEqual(_putToBlock, currentRecord.Block);
            Assert.AreEqual(4, currentRecord.Offset);
        }

        [Test]
        public void CanWriteSetIntRecordMultipleTimesToLog()
        {
            _logRecord = new SetIntRecord(
                       _logManager,
                       _bufferManager,
                       1,
                       _putToBlock,
                       0,
                       123);
            var logRecord2 = new SetIntRecord(
                       _logManager,
                       _bufferManager,
                       1,
                       _putToBlock,
                       0,
                       321);

            _ = _logRecord.WriteToLog();
            var lsn2 = logRecord2.WriteToLog();

            _logManager.Flush(lsn2);

            var enumerator = _logManager.GetEnumerator();
            var record = enumerator.Current;
            var currentRecord2 = new SetIntRecord(_logManager, _bufferManager, record);
            enumerator.MoveNext();
            record = enumerator.Current;
            var currentRecord1 = new SetIntRecord(_logManager, _bufferManager, record);

            Assert.IsNotNull(currentRecord2);
            Assert.AreEqual(LogRecordType.SetInt, currentRecord2.Type);
            Assert.AreEqual(1, currentRecord1.TransactionNumber);
            Assert.AreEqual(321, currentRecord2.Value);
            Assert.AreEqual(_putToBlock, currentRecord2.Block);
            Assert.AreEqual(0, currentRecord2.Offset);

            Assert.IsNotNull(currentRecord1);
            Assert.AreEqual(LogRecordType.SetInt, currentRecord1.Type);
            Assert.AreEqual(1, currentRecord1.TransactionNumber);
            Assert.AreEqual(123, currentRecord1.Value);
            Assert.AreEqual(_putToBlock, currentRecord1.Block);
            Assert.AreEqual(0, currentRecord1.Offset);
        }

        [Test]
        public void SetIntCanPerformUndo()
        {
            _logRecord = new SetIntRecord(
                      _logManager,
                      _bufferManager,
                      3,
                      _putToBlock,
                      6,
                      123);

            var lsn = _logRecord.WriteToLog();
            _logManager.Flush(lsn);

            _logRecord.Undo();
            _bufferManager.FlushAll(3);

            var page = _fileManager.ResolvePage();
            page.Read(_putToBlock);
            var canRead = page.GetInt(6, out var result);

            Assert.IsTrue(canRead);
            Assert.AreEqual(123, result);            
        }

        [Test]
        public void SetIntRecordToStringContainsMeaningfullInformation()
        {
            _logRecord = new SetIntRecord(
                       _logManager,
                       _bufferManager,
                       3,
                       _putToBlock,
                       6,
                       123);

            var result = _logRecord
                .ToString()
                .ToUpper();

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Contains("SETINT"));
            Assert.IsTrue(result.Contains("3"));
            Assert.IsTrue(result.Contains("6"));
            Assert.IsTrue(result.Contains("123"));
            Assert.IsTrue(result.Contains(_putToBlock.Id.ToString()));
            Assert.IsTrue(result.Contains(_putToBlock.Filename.ToString().ToUpper()));
        }

        [OneTimeTearDown]
        public void ClearDirectory()
        {
            Directory.Delete("DBs", true);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
