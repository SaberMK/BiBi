using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Transactions.Abstract;
using BB.Transactions.Records;
using BB.Transactions.Recovery;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace BB.Transactions.Tests.Recovery
{
    public class LogRecordEnumeratorTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private IRecoveryManager _recoveryManager;
        private IBufferManager _bufferManager;
        private IEnumerator<LogRecord> _enumerator;
        private string _logName;
        [SetUp]
        public void Setup()
        {
            _logName = RandomFilename;
            _fileManager = new FileManager("temp", "DBs", 100);
            _logManager = new LogManager(_fileManager, _logName);
            _bufferManager = new BufferManager(_fileManager, _logManager, new NaiveBufferPoolStrategy(_logManager, _fileManager, 5));
        }

        [Test]
        public void CanCreateAndDisposeLogEnumerator()
        {
            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 5);
            var block = new Block(RandomFilename, 0);
            var buffer = _bufferManager.Pin(block);
            _recoveryManager.SetInt(buffer, 12, 12);
            _recoveryManager.Commit();

            Assert.DoesNotThrow(() =>
            {
                _enumerator = new LogRecordEnumerator(_logManager, _bufferManager);
                _enumerator.Dispose();
            });
        }

        [Test]
        public void CanReadStartRecord()
        {
            var rec = new StartRecord(_logManager, _bufferManager, 5);
            var lsn = rec.WriteToLog();
            _logManager.Flush(lsn);


            _enumerator = new LogRecordEnumerator(_logManager, _bufferManager);
            var record = _enumerator.Current as StartRecord;

            Assert.AreEqual(LogRecordType.Start, record.Type);
            Assert.AreEqual(5, record.TransactionNumber);
        }

        [Test]
        public void CanReadCommitRecord()
        {
            var rec = new CommitRecord(_logManager, _bufferManager, 5);
            var lsn = rec.WriteToLog();
            _logManager.Flush(lsn);


            _enumerator = new LogRecordEnumerator(_logManager, _bufferManager);
            var record = _enumerator.Current as CommitRecord;

            Assert.AreEqual(LogRecordType.Commit, record.Type);
            Assert.AreEqual(5, record.TransactionNumber);
        }

        [Test]
        public void CanReadRollbackRecord()
        {
            var rec = new RollbackRecord(_logManager, _bufferManager, 5);
            var lsn = rec.WriteToLog();
            _logManager.Flush(lsn);


            _enumerator = new LogRecordEnumerator(_logManager, _bufferManager);
            var record = _enumerator.Current as RollbackRecord;

            Assert.AreEqual(LogRecordType.Rollback, record.Type);
            Assert.AreEqual(5, record.TransactionNumber);
        }

        [Test]
        public void CanReadCheckpointRecord()
        {
            var rec = new CheckpointRecord(_logManager, _bufferManager, new int[] { 5,8,9});
            var lsn = rec.WriteToLog();
            _logManager.Flush(lsn);


            _enumerator = new LogRecordEnumerator(_logManager, _bufferManager);
            var record = _enumerator.Current as CheckpointRecord;

            Assert.AreEqual(LogRecordType.Checkpoint, record.Type);
            Assert.AreEqual(new int[] { 5, 8, 9 }, record.Transactions);
        }

        [Test]
        public void CanReadSetIntRecord()
        {
            var block = new Block(RandomFilename, 0);
            var rec = new SetIntRecord(_logManager, _bufferManager, 5, block, 0, 123);
            var lsn = rec.WriteToLog();
            _logManager.Flush(lsn);


            _enumerator = new LogRecordEnumerator(_logManager, _bufferManager);
            var record = _enumerator.Current as SetIntRecord;

            Assert.AreEqual(LogRecordType.SetInt, record.Type);
            Assert.AreEqual(5, record.TransactionNumber);
            Assert.AreEqual(block, record.Block);
            Assert.AreEqual(0, record.Offset);
            Assert.AreEqual(123, record.Value);
        }


        [Test]
        public void CanReadSetByteRecord()
        {
            var block = new Block(RandomFilename, 0);
            var rec = new SetByteRecord(_logManager, _bufferManager, 5, block, 0, 123);
            var lsn = rec.WriteToLog();
            _logManager.Flush(lsn);


            _enumerator = new LogRecordEnumerator(_logManager, _bufferManager);
            var record = _enumerator.Current as SetByteRecord;

            Assert.AreEqual(LogRecordType.SetByte, record.Type);
            Assert.AreEqual(5, record.TransactionNumber);
            Assert.AreEqual(block, record.Block);
            Assert.AreEqual(0, record.Offset);
            Assert.AreEqual(123, record.Value);
        }

        [Test]
        public void CanReadSetBoolRecord()
        {
            var block = new Block(RandomFilename, 0);
            var rec = new SetBoolRecord(_logManager, _bufferManager, 5, block, 0, true);
            var lsn = rec.WriteToLog();
            _logManager.Flush(lsn);


            _enumerator = new LogRecordEnumerator(_logManager, _bufferManager);
            var record = _enumerator.Current as SetBoolRecord;

            Assert.AreEqual(LogRecordType.SetBool, record.Type);
            Assert.AreEqual(5, record.TransactionNumber);
            Assert.AreEqual(block, record.Block);
            Assert.AreEqual(0, record.Offset);
            Assert.AreEqual(true, record.Value);
        }

        [Test]
        public void CanReadSetBlobRecord()
        {
            var block = new Block(RandomFilename, 0);
            var rec = new SetBlobRecord(_logManager, _bufferManager, 5, block, 0, new byte[] { 1, 2, 3 });
            var lsn = rec.WriteToLog();
            _logManager.Flush(lsn);


            _enumerator = new LogRecordEnumerator(_logManager, _bufferManager);
            var record = _enumerator.Current as SetBlobRecord;

            Assert.AreEqual(LogRecordType.SetBlob, record.Type);
            Assert.AreEqual(5, record.TransactionNumber);
            Assert.AreEqual(block, record.Block);
            Assert.AreEqual(0, record.Offset);
            Assert.AreEqual(new byte[] { 1, 2, 3 }, record.Value);
        }

        [Test]
        public void CanReadSetStringRecord()
        {
            var block = new Block(RandomFilename, 0);
            var rec = new SetStringRecord(_logManager, _bufferManager, 5, block, 0, "default string");
            var lsn = rec.WriteToLog();
            _logManager.Flush(lsn);


            _enumerator = new LogRecordEnumerator(_logManager, _bufferManager);
            var record = _enumerator.Current as SetStringRecord;

            Assert.AreEqual(LogRecordType.SetString, record.Type);
            Assert.AreEqual(5, record.TransactionNumber);
            Assert.AreEqual(block, record.Block);
            Assert.AreEqual(0, record.Offset);
            Assert.AreEqual("default string", record.Value);
        }

        [Test]
        public void CanReadSetDateRecord()
        {
            var block = new Block(RandomFilename, 0);
            var rec = new SetDateRecord(_logManager, _bufferManager, 5, block, 0, new DateTime(2020, 1, 1));
            var lsn = rec.WriteToLog();
            _logManager.Flush(lsn);


            _enumerator = new LogRecordEnumerator(_logManager, _bufferManager);
            var record = _enumerator.Current as SetDateRecord;

            Assert.AreEqual(LogRecordType.SetDate, record.Type);
            Assert.AreEqual(5, record.TransactionNumber);
            Assert.AreEqual(block, record.Block);
            Assert.AreEqual(0, record.Offset);
            Assert.AreEqual(new DateTime(2020, 1, 1), record.Value);
        }

        [Test]
        public void CannotReadDamagedRecord()
        {
            var block = new Block(RandomFilename, 0);
            var rec = new SetIntRecord(_logManager, _bufferManager, 5, block, 0, 123);
            var lsn = rec.WriteToLog();
            _logManager.Flush(lsn);

            var page = _fileManager.ResolvePage(new Block(_logName, 0));
            page.Read(new Block(_logName, 0));
            
            // This is not a valid log record type
            page.SetInt(4, int.MaxValue);
            page.Write(new Block(_logName, 0));

            var lm = new LogManager(_fileManager, _logName);

            _enumerator = new LogRecordEnumerator(lm, _bufferManager);
            var record = _enumerator.Current;

            Assert.IsNull(record);
        }

        [Test]
        public void CanGetLegacyEnumerator()
        {
            var rec = new StartRecord(_logManager, _bufferManager, 5);
            var lsn = rec.WriteToLog();
            _logManager.Flush(lsn);


            _enumerator = new LogRecordEnumerator(_logManager, _bufferManager);
            var record = ((IEnumerator)_enumerator).Current;

            Assert.IsNotNull(record);
        }

        [Test]
        public void CanReadMultipleEntries()
        {
            var rec1 = new StartRecord(_logManager, _bufferManager, 5);
            var lsn1 = rec1.WriteToLog();

            var rec2 = new StartRecord(_logManager, _bufferManager, 5);
            var lsn2 = rec2.WriteToLog();

            var rec3 = new StartRecord(_logManager, _bufferManager, 5);
            var lsn3 = rec3.WriteToLog();

            _logManager.Flush(lsn3);

            _enumerator = new LogRecordEnumerator(_logManager, _bufferManager);

            var canRead1 = _enumerator.MoveNext();
            var canRead2 = _enumerator.MoveNext();
            var canRead3 = _enumerator.MoveNext();

            Assert.IsTrue(canRead1);
            Assert.IsTrue(canRead2);
            Assert.IsFalse(canRead3);
        }

        [Test]
        public void CanReadMultipleEntriesResetAndReadAgain()
        {
            var rec1 = new StartRecord(_logManager, _bufferManager, 5);
            var lsn1 = rec1.WriteToLog();

            var rec2 = new StartRecord(_logManager, _bufferManager, 5);
            var lsn2 = rec2.WriteToLog();

            var rec3 = new StartRecord(_logManager, _bufferManager, 5);
            var lsn3 = rec3.WriteToLog();

            _logManager.Flush(lsn3);

            _enumerator = new LogRecordEnumerator(_logManager, _bufferManager);

            var canRead1 = _enumerator.MoveNext();
            var canRead2 = _enumerator.MoveNext();
            var canRead3 = _enumerator.MoveNext();

            _enumerator.Reset();

            var canRead4 = _enumerator.MoveNext();
            var canRead5 = _enumerator.MoveNext();
            var canRead6 = _enumerator.MoveNext();

            Assert.IsTrue(canRead1);
            Assert.IsTrue(canRead2); 
            Assert.IsFalse(canRead3);

            Assert.IsTrue(canRead4);
            Assert.IsTrue(canRead5);
            Assert.IsFalse(canRead6);
        }

        [OneTimeTearDown]
        public void ClearDirectory()
        {
            Directory.Delete("DBs", true);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
