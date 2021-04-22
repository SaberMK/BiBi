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
using System.Collections.Generic;
using System.IO;

namespace BB.Tests.Transactions.Recovery
{
    public class RecoveryManagerTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private IEnumerator<LogRecord> _enumerator;
        private IRecoveryManager _recoveryManager;
        private IBufferManager _bufferManager;
        private string _name;

        [SetUp]
        public void Setup()
        {
            _name = RandomFilename;
            _fileManager = new FileManager("temp", "DBs", 100);
            _logManager = new LogManager(_fileManager, _name);
            _bufferManager = new BufferManager(_fileManager, _logManager, new NaiveBufferPoolStrategy(_logManager, _fileManager, 5));
        }

        [Test]
        public void CanCreateRecoveryManager()
        {
            Assert.DoesNotThrow(() =>
            {
                _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 5);
            });
        }

        [Test]
        public void CanRecover()
        {
            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var block = new Block(RandomFilename, 0);

            var buffer = _bufferManager.Pin(block);

            _recoveryManager.Recover();

            _bufferManager.Unpin(buffer);
            _bufferManager.FlushAll(6);

            var page = _fileManager.ResolvePage();
            _ = page.Read(new Block(_name, 0));

            var enumerator = _logManager.GetEnumerator();

            var record = enumerator.Current;
            var checkpointRecord = new CheckpointRecord(_logManager, _bufferManager, record);

            Assert.AreEqual(LogRecordType.Checkpoint, checkpointRecord.Type);
            Assert.AreEqual(1, checkpointRecord.Transactions.Length);
            Assert.AreEqual(6, checkpointRecord.Transactions[0]);
        }


        [Test]
        public void CanCommit()
        {
            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var block = new Block(RandomFilename, 0);

            var buffer = _bufferManager.Pin(block);

            _recoveryManager.Commit();

            _bufferManager.Unpin(buffer);
            _bufferManager.FlushAll(6);

            var page = _fileManager.ResolvePage();
            _ = page.Read(new Block(_name, 0));

            var enumerator = _logManager.GetEnumerator();

            var record = enumerator.Current;
            var commitRecord = new CommitRecord(_logManager, _bufferManager, record);

            Assert.AreEqual(LogRecordType.Commit, commitRecord.Type);
            Assert.AreEqual(6, commitRecord.TransactionNumber);
        }


        [Test]
        public void CanRollback()
        {
            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var block = new Block(RandomFilename, 0);

            var buffer = _bufferManager.Pin(block);

            _recoveryManager.Rollback();

            _bufferManager.Unpin(buffer);
            _bufferManager.FlushAll(6);

            var page = _fileManager.ResolvePage();
            _ = page.Read(new Block(_name, 0));

            var enumerator = _logManager.GetEnumerator();

            var record = enumerator.Current;
            var commitRecord = new RollbackRecord(_logManager, _bufferManager, record);

            Assert.AreEqual(LogRecordType.Rollback, commitRecord.Type);
            Assert.AreEqual(6, commitRecord.TransactionNumber);
        }

        [Test]
        public void CanSetInt()
        {
            var page = _fileManager.ResolvePage();

            var block = new Block(RandomFilename, 0);
            page.Read(block);
            page.SetInt(0, 222);
            page.Write(block);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var buffer = _bufferManager.Pin(block);


            var lsn = _recoveryManager.SetInt(buffer, 0, 123);
            _logManager.Flush(lsn);

            _bufferManager.Unpin(buffer);
            _bufferManager.FlushAll(6);

            var enumerator = _logManager.GetEnumerator();

            var record = enumerator.Current;
            var setIntRecord = new SetIntRecord(_logManager, _bufferManager, record);

            Assert.AreEqual(LogRecordType.SetInt, setIntRecord.Type);
            Assert.AreEqual(0, setIntRecord.Block.Id);
            Assert.AreEqual(0, setIntRecord.Offset);
            Assert.AreEqual(222, setIntRecord.Value);
            Assert.AreEqual(6, setIntRecord.TransactionNumber);
        }

        [Test]
        public void CanSetBool()
        {
            var page = _fileManager.ResolvePage();

            var block = new Block(RandomFilename, 0);
            page.Read(block);
            page.SetBool(0, true);
            page.Write(block);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var buffer = _bufferManager.Pin(block);


            var lsn = _recoveryManager.SetBool(buffer, 0, true);
            _logManager.Flush(lsn);

            _bufferManager.Unpin(buffer);
            _bufferManager.FlushAll(6);

            var enumerator = _logManager.GetEnumerator();

            var record = enumerator.Current;
            var setBoolRecord = new SetBoolRecord(_logManager, _bufferManager, record);

            Assert.AreEqual(LogRecordType.SetBool, setBoolRecord.Type);
            Assert.AreEqual(0, setBoolRecord.Block.Id);
            Assert.AreEqual(0, setBoolRecord.Offset);
            Assert.AreEqual(true, setBoolRecord.Value);
            Assert.AreEqual(6, setBoolRecord.TransactionNumber);
        }

        [Test]
        public void CanSetByte()
        {
            var page = _fileManager.ResolvePage();

            var block = new Block(RandomFilename, 0);
            page.Read(block);
            page.SetByte(0, 32);
            page.Write(block);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var buffer = _bufferManager.Pin(block);

            var lsn = _recoveryManager.SetByte(buffer, 0, 123);
            _logManager.Flush(lsn);

            _bufferManager.Unpin(buffer);
            _bufferManager.FlushAll(6);

            var enumerator = _logManager.GetEnumerator();

            var record = enumerator.Current;
            var setBoolRecord = new SetByteRecord(_logManager, _bufferManager, record);

            Assert.AreEqual(LogRecordType.SetByte, setBoolRecord.Type);
            Assert.AreEqual(0, setBoolRecord.Block.Id);
            Assert.AreEqual(0, setBoolRecord.Offset);
            Assert.AreEqual(32, setBoolRecord.Value);
            Assert.AreEqual(6, setBoolRecord.TransactionNumber);
        }

        [Test]
        public void CanSetDate()
        {
            var page = _fileManager.ResolvePage();

            var block = new Block(RandomFilename, 0);
            page.Read(block);
            page.SetDate(0, new DateTime(2020, 1, 1));
            page.Write(block);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var buffer = _bufferManager.Pin(block);

            var lsn = _recoveryManager.SetDate(buffer, 0, new DateTime(2000, 1, 1));
            _logManager.Flush(lsn);

            _bufferManager.Unpin(buffer);
            _bufferManager.FlushAll(6);

            var enumerator = _logManager.GetEnumerator();

            var record = enumerator.Current;
            var setBoolRecord = new SetDateRecord(_logManager, _bufferManager, record);

            Assert.AreEqual(LogRecordType.SetDate, setBoolRecord.Type);
            Assert.AreEqual(0, setBoolRecord.Block.Id);
            Assert.AreEqual(0, setBoolRecord.Offset);
            Assert.AreEqual(new DateTime(2020, 1, 1), setBoolRecord.Value);
            Assert.AreEqual(6, setBoolRecord.TransactionNumber);
        }

        [Test]
        public void CanSetBlob()
        {
            var page = _fileManager.ResolvePage();

            var block = new Block(RandomFilename, 0);
            page.Read(block);
            page.SetBlob(0, new byte[] { 1, 2, 3 });
            page.Write(block);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var buffer = _bufferManager.Pin(block);

            var lsn = _recoveryManager.SetBlob(buffer, 0, new byte[] { 3, 2, 1 });
            _logManager.Flush(lsn);

            _bufferManager.Unpin(buffer);
            _bufferManager.FlushAll(6);

            var enumerator = _logManager.GetEnumerator();

            var record = enumerator.Current;
            var setBoolRecord = new SetBlobRecord(_logManager, _bufferManager, record);

            Assert.AreEqual(LogRecordType.SetBlob, setBoolRecord.Type);
            Assert.AreEqual(0, setBoolRecord.Block.Id);
            Assert.AreEqual(0, setBoolRecord.Offset);
            Assert.AreEqual(new byte[] { 1, 2, 3 }, setBoolRecord.Value);
            Assert.AreEqual(6, setBoolRecord.TransactionNumber);
        }

        [Test]
        public void CanSetString()
        {
            var page = _fileManager.ResolvePage();

            var block = new Block(RandomFilename, 0);
            page.Read(block);
            page.SetString(0, "default string");
            page.Write(block);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var buffer = _bufferManager.Pin(block);

            var lsn = _recoveryManager.SetString(buffer, 0, "not default string");
            _logManager.Flush(lsn);

            _bufferManager.Unpin(buffer);
            _bufferManager.FlushAll(6);

            var enumerator = _logManager.GetEnumerator();

            var record = enumerator.Current;
            var setBoolRecord = new SetStringRecord(_logManager, _bufferManager, record);

            Assert.AreEqual(LogRecordType.SetString, setBoolRecord.Type);
            Assert.AreEqual(0, setBoolRecord.Block.Id);
            Assert.AreEqual(0, setBoolRecord.Offset);
            Assert.AreEqual("default string", setBoolRecord.Value);
            Assert.AreEqual(6, setBoolRecord.TransactionNumber);
        }

        [Test]
        public void CanSetTempInt()
        {
            var page = _fileManager.ResolvePage();

            var block = new Block("temp" + RandomFilename, 0);
            page.Read(block);
            page.SetInt(0, 222);
            page.Write(block);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var buffer = _bufferManager.Pin(block);

            var lsn = _recoveryManager.SetInt(buffer, 0, 123);
            Assert.AreEqual(-1, lsn);
        }

        [Test]
        public void CanSetTempByte()
        {
            var page = _fileManager.ResolvePage();

            var block = new Block("temp" + RandomFilename, 0);
            page.Read(block);
            page.SetByte(0, 123);
            page.Write(block);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var buffer = _bufferManager.Pin(block);

            var lsn = _recoveryManager.SetByte(buffer, 0, 123);
            Assert.AreEqual(-1, lsn);
        }

        [Test]
        public void CanSetTempBool()
        {
            var page = _fileManager.ResolvePage();

            var block = new Block("temp" + RandomFilename, 0);
            page.Read(block);
            page.SetBool(0, true);
            page.Write(block);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var buffer = _bufferManager.Pin(block);

            var lsn = _recoveryManager.SetBool(buffer, 0, false);
            Assert.AreEqual(-1, lsn);
        }


        [Test]
        public void CanSetTempDate()
        {
            var page = _fileManager.ResolvePage();

            var block = new Block("temp" + RandomFilename, 0);
            page.Read(block);
            page.SetDate(0, new DateTime(2020, 1, 1));
            page.Write(block);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var buffer = _bufferManager.Pin(block);

            var lsn = _recoveryManager.SetDate(buffer, 0, new DateTime(2000, 1, 1));
            Assert.AreEqual(-1, lsn);
        }

        [Test]
        public void CanSetTempBlob()
        {
            var page = _fileManager.ResolvePage();

            var block = new Block("temp" + RandomFilename, 0);
            page.Read(block);
            page.SetBlob(0, new byte[] { 1, 2, 3 });
            page.Write(block);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var buffer = _bufferManager.Pin(block);

            var lsn = _recoveryManager.SetBlob(buffer, 0,new byte[] { 3, 2, 1 });
            Assert.AreEqual(-1, lsn);
        }

        [Test]
        public void CanSetTempString()
        {
            var page = _fileManager.ResolvePage();

            var block = new Block("temp" + RandomFilename, 0);
            page.Read(block);
            page.SetString(0, "default string");
            page.Write(block);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);
            var buffer = _bufferManager.Pin(block);

            var lsn = _recoveryManager.SetString(buffer, 0, "string default");
            Assert.AreEqual(-1, lsn);
        }

        [Test]
        public void CanRollbackTransaction()
        {
            var startRecord = new StartRecord(_logManager, _bufferManager, 6);

            var lsn = startRecord.WriteToLog();
            _logManager.Flush(lsn);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);

            var block = new Block(RandomFilename, 0);
            var buffer = _bufferManager.Pin(block);
            _recoveryManager.SetInt(buffer, 0, 123);

            Assert.DoesNotThrow(() =>
            {
                _recoveryManager.Rollback();
                _bufferManager.Unpin(buffer);
            });
        }


        [Test]
        public void CanRecoverSimpleTransaction()
        {
            var startRecord = new StartRecord(_logManager, _bufferManager, 6);

            var lsn = startRecord.WriteToLog();
            _logManager.Flush(lsn);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 6);

            var block = new Block(RandomFilename, 0);
            var buffer = _bufferManager.Pin(block);
            _recoveryManager.SetInt(buffer, 0, 123);

            Assert.DoesNotThrow(() =>
            {
                _recoveryManager.Recover();
                _bufferManager.Unpin(buffer);
            });
        }


        [Test]
        public void CanRecoverFromUncommitedAndCommitedTransactions()
        {
            var page = _fileManager.ResolvePage();
            var block = new Block(RandomFilename, 0);
            page.Read(block);
            page.SetInt(0, 222);
            page.Write(block);


            var startRecord = new StartRecord(_logManager, _bufferManager, 1);

            var lsn = startRecord.WriteToLog();
            _logManager.Flush(lsn);

            var startRecord2 = new StartRecord(_logManager, _bufferManager, 2);
            var lsn2 = startRecord2.WriteToLog();
            _logManager.Flush(lsn2);

            _recoveryManager = new RecoveryManager(_bufferManager, _logManager, 1);

            var buffer = _bufferManager.Pin(block);
            _recoveryManager.SetInt(buffer, 0, 123);
            _recoveryManager.SetByte(buffer, 0, 1);
            _recoveryManager.Commit();

            var recoveryManager2 = new RecoveryManager(_bufferManager, _logManager, 2);
            recoveryManager2.SetByte(buffer, 1, 0);
            
            Assert.DoesNotThrow(() =>
            {
                recoveryManager2.Recover();
                _bufferManager.Unpin(buffer);
            });

            page.Read(block);

            _ = page.GetInt(0, out var resultValue);
            Assert.AreEqual(222, resultValue);
        }

        [OneTimeTearDown]
        public void ClearDirectory()
        {
            Directory.Delete("DBs", true);
        }


        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
