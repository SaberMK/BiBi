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

namespace BB.Tests.Transactions.Records.Sets
{
    public class SetBlobRecordTests
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
        public void CanCreateSetBlobRecord()
        {
            Assert.DoesNotThrow(() =>
            {
                _logRecord = new SetBlobRecord(
                    _logManager,
                    _bufferManager,
                    1,
                    _putToBlock,
                    0,
                    new byte[] { 1, 2, 3 });
            });
        }

        [Test]
        public void CanWriteSetBlobRecordToLog()
        {
            _logRecord = new SetBlobRecord(
                       _logManager,
                       _bufferManager,
                       3,
                       _putToBlock,
                       4,
                       new byte[] { 1, 2, 3 });
            var lsn = _logRecord.WriteToLog();

            _logManager.Flush(lsn);


            var enumerator = _logManager.GetEnumerator();
            var record = enumerator.Current;
            var currentRecord = new SetBlobRecord(_logManager, _bufferManager, record);

            Assert.IsNotNull(currentRecord);
            Assert.AreEqual(LogRecordType.SetBlob, currentRecord.Type);
            Assert.AreEqual(3, currentRecord.TransactionNumber);
            Assert.AreEqual(new byte[] { 1, 2, 3 }, currentRecord.Value);
            Assert.AreEqual(_putToBlock, currentRecord.Block);
            Assert.AreEqual(4, currentRecord.Offset);
        }

        [Test]
        public void CanWriteSetBlobRecordMultipleTimesToLog()
        {
            _logRecord = new SetBlobRecord(
                       _logManager,
                       _bufferManager,
                       1,
                       _putToBlock,
                       0,
                       new byte[] { 4, 5, 6 });
            var logRecord2 = new SetBlobRecord(
                       _logManager,
                       _bufferManager,
                       1,
                       _putToBlock,
                       0,
                       new byte[] { 1, 2, 3 });

            _ = _logRecord.WriteToLog();
            var lsn2 = logRecord2.WriteToLog();

            _logManager.Flush(lsn2);

            var enumerator = _logManager.GetEnumerator();
            var record = enumerator.Current;
            var currentRecord2 = new SetBlobRecord(_logManager, _bufferManager, record);
            enumerator.MoveNext();
            record = enumerator.Current;
            var currentRecord1 = new SetBlobRecord(_logManager, _bufferManager, record);

            Assert.IsNotNull(currentRecord2);
            Assert.AreEqual(LogRecordType.SetBlob, currentRecord2.Type);
            Assert.AreEqual(1, currentRecord1.TransactionNumber);
            Assert.AreEqual(new byte[] { 1, 2, 3 }, currentRecord2.Value);
            Assert.AreEqual(_putToBlock, currentRecord2.Block);
            Assert.AreEqual(0, currentRecord2.Offset);

            Assert.IsNotNull(currentRecord1);
            Assert.AreEqual(LogRecordType.SetBlob, currentRecord1.Type);
            Assert.AreEqual(1, currentRecord1.TransactionNumber);
            Assert.AreEqual(new byte[] { 4, 5, 6 }, currentRecord1.Value);
            Assert.AreEqual(_putToBlock, currentRecord1.Block);
            Assert.AreEqual(0, currentRecord1.Offset);
        }

        [Test]
        public void SetBlobCanPerformUndo()
        {
            _logRecord = new SetBlobRecord(
                      _logManager,
                      _bufferManager,
                      3,
                      _putToBlock,
                      6,
                      new byte[] { 1, 2, 3 });

            var lsn = _logRecord.WriteToLog();
            _logManager.Flush(lsn);

            _logRecord.Undo();
            _bufferManager.FlushAll(3);

            var page = _fileManager.ResolvePage();
            page.Read(_putToBlock);
            var canRead = page.GetBlob(6, out var result);

            Assert.IsTrue(canRead);
            Assert.AreEqual(new byte[] { 1, 2, 3 }, result);
        }

        [Test]
        public void SetBlobRecordToStringContainsMeaningfullInformation()
        {
            _logRecord = new SetBlobRecord(
                       _logManager,
                       _bufferManager,
                       3,
                       _putToBlock,
                       6,
                       new byte[] { 1, 2, 3 });

            var result = _logRecord
                .ToString()
                .ToUpper();

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Contains("SETBLOB"));
            Assert.IsTrue(result.Contains("3"));
            Assert.IsTrue(result.Contains("6"));
            Assert.IsTrue(result.Contains("1, 2, 3"));
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
