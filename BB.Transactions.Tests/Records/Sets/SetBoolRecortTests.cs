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


namespace BB.Transactions.Tests.Records.Sets
{
    public class SetBoolRecordTests
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
        public void CanCreateSetBoolRecord()
        {
            Assert.DoesNotThrow(() =>
            {
                _logRecord = new SetBoolRecord(
                    _logManager,
                    _bufferManager,
                    1,
                    _putToBlock,
                    0,
                    true);
            });
        }

        [Test]
        public void CanWriteSetBoolRecordToLog()
        {
            _logRecord = new SetBoolRecord(
                       _logManager,
                       _bufferManager,
                       3,
                       _putToBlock,
                       4,
                       true);
            var lsn = _logRecord.WriteToLog();

            _logManager.Flush(lsn);

            var logRecord = new LogRecordEnumerator(_logManager, _bufferManager);
            var currentRecord = logRecord.Current as SetBoolRecord;

            Assert.IsNotNull(currentRecord);
            Assert.AreEqual(LogRecordType.SetBool, currentRecord.Type);
            Assert.AreEqual(3, currentRecord.TransactionNumber);
            Assert.AreEqual(true, currentRecord.Value);
            Assert.AreEqual(_putToBlock, currentRecord.Block);
            Assert.AreEqual(4, currentRecord.Offset);
        }

        [Test]
        public void CanWriteSetBoolRecordMultipleTimesToLog()
        {
            _logRecord = new SetBoolRecord(
                       _logManager,
                       _bufferManager,
                       1,
                       _putToBlock,
                       0,
                       true);
            var logRecord2 = new SetBoolRecord(
                       _logManager,
                       _bufferManager,
                       1,
                       _putToBlock,
                       0,
                       false);

            _ = _logRecord.WriteToLog();
            var lsn2 = logRecord2.WriteToLog();

            _logManager.Flush(lsn2);

            var logRecord = new LogRecordEnumerator(_logManager, _bufferManager);
            var currentRecord2 = logRecord.Current as SetBoolRecord;
            logRecord.MoveNext();
            var currentRecord1 = logRecord.Current as SetBoolRecord;

            Assert.IsNotNull(currentRecord2);
            Assert.AreEqual(LogRecordType.SetBool, currentRecord2.Type);
            Assert.AreEqual(1, currentRecord1.TransactionNumber);
            Assert.AreEqual(false, currentRecord2.Value);
            Assert.AreEqual(_putToBlock, currentRecord2.Block);
            Assert.AreEqual(0, currentRecord2.Offset);

            Assert.IsNotNull(currentRecord1);
            Assert.AreEqual(LogRecordType.SetBool, currentRecord1.Type);
            Assert.AreEqual(1, currentRecord1.TransactionNumber);
            Assert.AreEqual(true, currentRecord1.Value);
            Assert.AreEqual(_putToBlock, currentRecord1.Block);
            Assert.AreEqual(0, currentRecord1.Offset);
        }

        [Test]
        public void SetBoolCanPerformUndo()
        {
            _logRecord = new SetBoolRecord(
                      _logManager,
                      _bufferManager,
                      3,
                      _putToBlock,
                      6,
                      true);

            var lsn = _logRecord.WriteToLog();
            _logManager.Flush(lsn);

            _logRecord.Undo();
            _bufferManager.FlushAll(3);

            var page = _fileManager.ResolvePage();
            page.Read(_putToBlock);
            var canRead = page.GetBool(6, out var result);

            Assert.IsTrue(canRead);
            Assert.AreEqual(true, result);
        }

        [Test]
        public void SetBoolRecordToStringContainsMeaningfullInformation()
        {
            _logRecord = new SetBoolRecord(
                       _logManager,
                       _bufferManager,
                       3,
                       _putToBlock,
                       6,
                       true);

            var result = _logRecord
                .ToString()
                .ToUpper();

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Contains("SETBOOL"));
            Assert.IsTrue(result.Contains("3"));
            Assert.IsTrue(result.Contains("6"));
            Assert.IsTrue(result.Contains("TRUE"));
            Assert.IsTrue(result.Contains(_putToBlock.Id.ToString()));
            Assert.IsTrue(result.Contains(_putToBlock.Filename.ToString().ToUpper()));
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
