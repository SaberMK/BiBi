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
    public class SetDateRecordTests
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
        public void CanCreateSetDateRecord()
        {
            Assert.DoesNotThrow(() =>
            {
                _logRecord = new SetDateRecord(
                    _logManager,
                    _bufferManager,
                    1,
                    _putToBlock,
                    0,
                    new DateTime(2020, 1, 1));
            });
        }

        [Test]
        public void CanWriteSetDateRecordToLog()
        {
            _logRecord = new SetDateRecord(
                       _logManager,
                       _bufferManager,
                       3,
                       _putToBlock,
                       4,
                       new DateTime(2020, 1, 1));
            var lsn = _logRecord.WriteToLog();

            _logManager.Flush(lsn);

            var logRecord = new LogRecordEnumerator(_logManager, _bufferManager);
            var currentRecord = logRecord.Current as SetDateRecord;

            Assert.IsNotNull(currentRecord);
            Assert.AreEqual(LogRecordType.SetDate, currentRecord.Type);
            Assert.AreEqual(3, currentRecord.TransactionNumber);
            Assert.AreEqual(new DateTime(2020, 1, 1), currentRecord.Value);
            Assert.AreEqual(_putToBlock, currentRecord.Block);
            Assert.AreEqual(4, currentRecord.Offset);
        }

        [Test]
        public void CanWriteSetDateRecordMultipleTimesToLog()
        {
            _logRecord = new SetDateRecord(
                       _logManager,
                       _bufferManager,
                       1,
                       _putToBlock,
                       0,
                       new DateTime(2020, 1, 1));
            var logRecord2 = new SetDateRecord(
                       _logManager,
                       _bufferManager,
                       1,
                       _putToBlock,
                       0,
                       new DateTime(2020, 1, 2));

            _ = _logRecord.WriteToLog();
            var lsn2 = logRecord2.WriteToLog();

            _logManager.Flush(lsn2);

            var logRecord = new LogRecordEnumerator(_logManager, _bufferManager);
            var currentRecord2 = logRecord.Current as SetDateRecord;
            logRecord.MoveNext();
            var currentRecord1 = logRecord.Current as SetDateRecord;

            Assert.IsNotNull(currentRecord2);
            Assert.AreEqual(LogRecordType.SetDate, currentRecord2.Type);
            Assert.AreEqual(1, currentRecord1.TransactionNumber);
            Assert.AreEqual(new DateTime(2020, 1, 2), currentRecord2.Value);
            Assert.AreEqual(_putToBlock, currentRecord2.Block);
            Assert.AreEqual(0, currentRecord2.Offset);

            Assert.IsNotNull(currentRecord1);
            Assert.AreEqual(LogRecordType.SetDate, currentRecord1.Type);
            Assert.AreEqual(1, currentRecord1.TransactionNumber);
            Assert.AreEqual(new DateTime(2020, 1, 1), currentRecord1.Value);
            Assert.AreEqual(_putToBlock, currentRecord1.Block);
            Assert.AreEqual(0, currentRecord1.Offset);
        }

        [Test]
        public void SetDateCanPerformUndo()
        {
            _logRecord = new SetDateRecord(
                      _logManager,
                      _bufferManager,
                      3,
                      _putToBlock,
                      6,
                      new DateTime(2020, 1, 1));

            var lsn = _logRecord.WriteToLog();
            _logManager.Flush(lsn);

            _logRecord.Undo();
            _bufferManager.FlushAll(3);

            var page = _fileManager.ResolvePage();
            page.Read(_putToBlock);
            var canRead = page.GetDate(6, out var result);

            Assert.IsTrue(canRead);
            Assert.AreEqual(new DateTime(2020, 1, 1), result);
        }

        [Test]
        public void SetDateRecordToStringContainsMeaningfullInformation()
        {
            _logRecord = new SetDateRecord(
                       _logManager,
                       _bufferManager,
                       3,
                       _putToBlock,
                       6,
                       new DateTime(2020, 1, 2));

            var result = _logRecord
                .ToString()
                .ToUpper();

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Contains("SETDATE"));
            Assert.IsTrue(result.Contains("3"));
            Assert.IsTrue(result.Contains("6"));
            Assert.IsTrue(result.Contains("2020"));
            Assert.IsTrue(result.Contains("1"));
            Assert.IsTrue(result.Contains("2"));
            Assert.IsTrue(result.Contains(_putToBlock.Id.ToString()));
            Assert.IsTrue(result.Contains(_putToBlock.Filename.ToString().ToUpper()));
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
