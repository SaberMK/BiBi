using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Formatters;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Transactions;
using BB.Transactions.Abstract;
using BB.Transactions.Concurrency;
using BB.Transactions.Records;
using BB.Transactions.Recovery;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace BB.Tests.Transactions
{
    public class TransactionTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private IRecoveryManager _recoveryManager;
        private IBufferManager _bufferManager;
        private IEnumerator<LogRecord> _enumerator;
        private ITransactionNumberDispatcher _dispatcher;
        private IConcurrencyManager _concurrencyManager;

        private string _logName;

        private Transaction _transaction;

        [SetUp]
        public void Setup()
        {
            _logName = RandomFilename;
            _fileManager = new FileManager("temp", "DBs", 100);
            _logManager = new LogManager(_fileManager, _logName);
            _bufferManager = new BufferManager(_fileManager, _logManager, new NaiveBufferPoolStrategy(_logManager, _fileManager, 5));
            _dispatcher = new TransactionNumberDispatcher(10);
            _concurrencyManager = new ConcurrencyManager();
        }

        [Test]
        public void CanCreateTransaction()
        {
            Assert.DoesNotThrow(() =>
            {
                _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);
            });
        }

        [Test]
        public void CanPinBlock()
        {
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            Assert.DoesNotThrow(() =>
            {
                _transaction.Pin(new Block(RandomFilename, 0));
            });
        }

        [Test]
        public void CanPinAndUnpinBlock()
        {
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            var block = new Block(RandomFilename, 0);

            Assert.DoesNotThrow(() =>
            {
                _transaction.Pin(block);
                _transaction.Unpin(block);
            });
        }

        [Test]
        public void CanSetInt()
        {
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);
            var block = new Block(RandomFilename, 0);

            _transaction.Pin(block);
            _transaction.SetInt(block, 0, 123);
            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            var canRead = page.GetInt(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(123, value);
        }

        [Test]
        public void CanSetByte()
        {
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);
            var block = new Block(RandomFilename, 0);

            _transaction.Pin(block);
            _transaction.SetByte(block, 0, 123);
            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            var canRead = page.GetByte(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(123, value);
        }

        [Test]
        public void CanSetBool()
        {
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);
            var block = new Block(RandomFilename, 0);

            _transaction.Pin(block);
            _transaction.SetBool(block, 0, true);
            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            var canRead = page.GetBool(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(true, value);
        }

        [Test]
        public void CanSetDate()
        {
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);
            var block = new Block(RandomFilename, 0);

            _transaction.Pin(block);
            _transaction.SetDate(block, 0, new DateTime(2020, 1, 1));
            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            var canRead = page.GetDate(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(new DateTime(2020, 1, 1), value);
        }

        [Test]
        public void CanSetBlob()
        {
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);
            var block = new Block(RandomFilename, 0);

            _transaction.Pin(block);
            _transaction.SetBlob(block, 0, new byte[] { 1, 2, 3 });
            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            var canRead = page.GetBlob(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(new byte[] { 1, 2, 3 }, value);
        }


        [Test]
        public void CanSetString()
        {
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);
            var block = new Block(RandomFilename, 0);

            _transaction.Pin(block);
            _transaction.SetString(block, 0, "test123");
            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            var canRead = page.GetBlob(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual("test123", value);
        }

        [Test]
        public void CanGetFileLength()
        {
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);
            var block = new Block(RandomFilename, 0);

            _transaction.Pin(block);
            _transaction.SetInt(block, 0, 123);

            //it is zero because we hadn't write anything
            var length = _transaction.Length(block.Filename);

            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            var canRead = page.GetInt(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(123, value);
            Assert.AreEqual(0, length);
        }

        [Test]
        public void CanSetRollback()
        {
            var block = new Block(RandomFilename, 0);
            var page = _fileManager.ResolvePage(block);
            page.SetInt(0, 333);
            page.Write(block);

            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);
            
            _transaction.Pin(block);
            _transaction.SetInt(block, 0, 123);
            _transaction.Rollback();
            _transaction.Recover();

            var pageAfterRollback = _fileManager.ResolvePage(block);
            pageAfterRollback.Read(block);

            var canRead = pageAfterRollback.GetInt(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(333, value);
        }

        [Test]
        public void CanAppendToFile()
        {
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);
            var block = new Block(RandomFilename, 0);

            _transaction.Pin(block);
            _transaction.SetInt(block, 0, 123);

            _transaction.Append(block.Filename, new BasePageFormatter());
            //it is zero because we hadn't write anything
            var length = _transaction.Length(block.Filename);

            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            var canRead = page.GetInt(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(123, value);
            Assert.AreEqual(1, length);
        }

        [Test]
        public void CanGetInt()
        {
            var block = new Block(RandomFilename, 0);
            var page = _fileManager.ResolvePage(block);
            page.SetInt(0, 333);
            page.Write(block);

            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            _transaction.Pin(block);
            var canRead = _transaction.GetInt(block, 0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(333, value);
        }

        [Test]
        public void CanGetByte()
        {
            var block = new Block(RandomFilename, 0);
            var page = _fileManager.ResolvePage(block);
            page.SetByte(0, 33);
            page.Write(block);

            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            _transaction.Pin(block);
            var canRead = _transaction.GetByte(block, 0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(33, value);
        }

        [Test]
        public void CanGetBool()
        {
            var block = new Block(RandomFilename, 0);
            var page = _fileManager.ResolvePage(block);
            page.SetBool(0, true);
            page.Write(block);

            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            _transaction.Pin(block);
            var canRead = _transaction.GetBool(block, 0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(true, value);
        }

        [Test]
        public void CanGetDate()
        {
            var block = new Block(RandomFilename, 0);
            var page = _fileManager.ResolvePage(block);
            page.SetDate(0, new DateTime(2020, 1, 1));
            page.Write(block);

            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            _transaction.Pin(block);
            var canRead = _transaction.GetDate(block, 0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(new DateTime(2020, 1, 1), value);
        }

        [Test]
        public void CanGetBlob()
        {
            var block = new Block(RandomFilename, 0);
            var page = _fileManager.ResolvePage(block);
            page.SetBlob(0, new byte[] { 1, 2, 3 });
            page.Write(block);

            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            _transaction.Pin(block);
            var canRead = _transaction.GetBlob(block, 0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(new byte[] { 1, 2, 3 }, value);
        }

        [Test]
        public void CanGetString()
        {
            var block = new Block(RandomFilename, 0);
            var page = _fileManager.ResolvePage(block);
            page.SetString(0, "123123");
            page.Write(block);

            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            _transaction.Pin(block);
            var canRead = _transaction.GetString(block, 0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual("123123", value);
        }


        [OneTimeTearDown]
        public void ClearDirectory()
        {
            try
            {
                Directory.Delete("DBs", true);
            }
            catch { }
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
