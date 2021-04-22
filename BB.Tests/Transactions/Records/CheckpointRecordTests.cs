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
    public class CheckpointRecordTests
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
        public void CanCreateCheckpointRecord()
        {
            Assert.DoesNotThrow(() =>
            {
                _logRecord = new CheckpointRecord(_logManager, _bufferManager, new int[] { 1 });
            });
        }

        [Test]
        public void CanWriteCheckpointRecordToLog()
        {
            _logRecord = new CheckpointRecord(_logManager, _bufferManager, new int[] { 5 });
            var lsn = _logRecord.WriteToLog();

            _logManager.Flush(lsn);
            var page = _fileManager.ResolvePage();
            _ = page.Read(new Block(_logFileName, 0));

            _ = page.GetInt(0, out var _);
            _ = page.GetInt(4, out var recordType);
            _ = page.GetInt(8, out var totalLength);

            var items = new int[totalLength];
            for (int i = 0; i < totalLength; ++i)
            {
                _ = page.GetInt(12 + i * 4, out items[i]);
            }

            Assert.AreEqual(0, lsn);
            Assert.AreEqual((int)LogRecordType.Checkpoint, recordType);
            Assert.AreEqual(1, totalLength);
            Assert.AreEqual(1, items.Length);
            Assert.AreEqual(5, items[0]);
        }

        [Test]
        public void CanWriteMultipleCheckpointRecordsToLog()
        {
            _logRecord = new CheckpointRecord(_logManager, _bufferManager, new int[] { 1 });
            var lsn = _logRecord.WriteToLog();
            var lsn2 = new CheckpointRecord(_logManager, _bufferManager, new int[] { 2, 3 }).WriteToLog();

            _logManager.Flush(lsn2);
            var page = _fileManager.ResolvePage();

            _ = page.Read(new Block(_logFileName, 0));

            _ = page.GetInt(0, out var _);
            _ = page.GetInt(4, out var recordType1);
            _ = page.GetInt(8, out var totalLength1);

            var items1 = new int[totalLength1];
            for (int i = 0; i < totalLength1; ++i)
            {
                _ = page.GetInt(12 + i * 4, out items1[i]);
            }

            _ = page.GetInt(20, out var recordType2);
            _ = page.GetInt(24, out var totalLength2);

            var items2 = new int[totalLength2];
            for (int i = 0; i < totalLength2; ++i)
            {
                _ = page.GetInt(28 + i * 4, out items2[i]);
            }


            Assert.AreEqual(0, lsn);
            Assert.AreEqual((int)LogRecordType.Checkpoint, recordType1);
            Assert.AreEqual(1, totalLength1);
            Assert.AreEqual(1, items1.Length);
            Assert.AreEqual(1, items1[0]);

            Assert.AreEqual(1, lsn2);
            Assert.AreEqual((int)LogRecordType.Checkpoint, recordType2);
            Assert.AreEqual(2, totalLength2);
            Assert.AreEqual(2, items2.Length);
            Assert.AreEqual(2, items2[0]);
            Assert.AreEqual(3, items2[1]);
        }

        [Test]
        public void CanReadBackwardsFromLog()
        {
            _logRecord = new CheckpointRecord(_logManager, _bufferManager, new int[] { 1, 2, 3 });
            var lsn = _logRecord.WriteToLog();

            _logManager.Flush(lsn);

            var enumerator = _logManager.GetEnumerator();
            var basicLogRecord = enumerator.Current;

            var logRecord = new CheckpointRecord(_logManager, _bufferManager, basicLogRecord);

            Assert.AreEqual(LogRecordType.Checkpoint, logRecord.Type);
            Assert.AreEqual(3, logRecord.Transactions.Length);
            Assert.AreEqual(1, logRecord.Transactions[0]);
            Assert.AreEqual(2, logRecord.Transactions[1]);
            Assert.AreEqual(3, logRecord.Transactions[2]);
        }

        [Test]
        public void CanUndoRecord()
        {
            Assert.DoesNotThrow(() =>
            {
                _logRecord = new CheckpointRecord(_logManager, _bufferManager, new int[] { 1, 2, 3 });
                _logRecord.Undo();
            });
        }

        [Test]
        public void ToStringReturnsMeaningfullInfo()
        {
            _logRecord = new CheckpointRecord(_logManager, _bufferManager, new int[] { 1, 2, 3 });

            var result = _logRecord
                .ToString()
                .ToUpper();

            Assert.IsTrue(result.Contains("CHECKPOINT"));
            Assert.IsTrue(result.Contains("1"));
            Assert.IsTrue(result.Contains("2"));
            Assert.IsTrue(result.Contains("3"));
        }

        [OneTimeTearDown]
        public void ClearDirectory()
        {
            Directory.Delete("DBs", true);
        }


        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
