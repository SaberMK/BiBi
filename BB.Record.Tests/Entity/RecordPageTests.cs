using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Record.Base;
using BB.Record.Entity;
using BB.Transactions;
using BB.Transactions.Abstract;
using BB.Transactions.Concurrency;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Record.Tests.Entity
{
    [TestFixture]
    public class RecordPageTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private IRecoveryManager _recoveryManager;
        private IBufferManager _bufferManager;
        private IEnumerator<LogRecord> _enumerator;
        private ITransactionNumberDispatcher _dispatcher;
        private IConcurrencyManager _concurrencyManager;

        private string _logName;
        private TableInfo _tableInfo;
        private Transaction _transaction;

        private RecordPage recordPage;

        [SetUp]
        public void Setup()
        {
            _logName = RandomFilename;
            _fileManager = new FileManager("temp", "DBs", 100);
            _logManager = new LogManager(_fileManager, _logName);
            _bufferManager = new BufferManager(_fileManager, _logManager, new NaiveBufferPoolStrategy(_logManager, _fileManager, 10));
            _dispatcher = new TransactionNumberDispatcher(10);
            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            var sch = new Schema();
            sch.AddIntField("field");
            _tableInfo = new TableInfo(RandomFilename, sch);
        }

        [Test]
        public void CanCreateRecordPage()
        {
            Assert.DoesNotThrow(() =>
            {
                recordPage = new RecordPage(new Block(RandomFilename, 0), _tableInfo, _transaction, _fileManager);
            });
        }

        [Test]
        public void CanClosePage()
        {
            Assert.DoesNotThrow(() =>
            {
                recordPage = new RecordPage(new Block(RandomFilename, 0), _tableInfo, _transaction, _fileManager);
                recordPage.Close();
            });

            Assert.AreEqual(-1, recordPage.CurrentId);
        }

        [Test]
        public void CannotFindNextRecordIfThereIsNone()
        {
            recordPage = new RecordPage(new Block(RandomFilename, 0), _tableInfo, _transaction, _fileManager);
            var canHaveNext = recordPage.Next();

            Assert.IsFalse(canHaveNext);
        }

        [Test]
        public void CanInsertRecordWithInt()
        {
            var fn = RandomFilename;
            var block = new Block(fn, 0);

            recordPage = new RecordPage(block, _tableInfo, _transaction, _fileManager);
            var canInsert = recordPage.Insert();
            recordPage.SetInt("field", 20);
            recordPage.Close();
            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            page.GetInt(0, out var isUsed);
            page.GetInt(4, out var value);

            Assert.IsTrue(canInsert);
            Assert.AreEqual(1, isUsed);
            Assert.AreEqual(20, value);
        }

        [Test]
        public void CanInsertRecordWithByte()
        {
            var fn = RandomFilename;
            var block = new Block(fn, 0);

            var sch = new Schema();
            sch.AddByteField("field");

            recordPage = new RecordPage(block, _tableInfo, _transaction, _fileManager);
            var canInsert = recordPage.Insert();
            recordPage.SetByte("field", 20);
            recordPage.Close();
            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            page.GetInt(0, out var isUsed);
            page.GetByte(4, out var value);

            Assert.IsTrue(canInsert);
            Assert.AreEqual(1, isUsed);
            Assert.AreEqual(20, value);
        }

        [Test]
        public void CanInsertRecordWithBool()
        {
            var fn = RandomFilename;
            var block = new Block(fn, 0);

            var sch = new Schema();
            sch.AddBoolField("field");

            recordPage = new RecordPage(block, _tableInfo, _transaction, _fileManager);
            var canInsert = recordPage.Insert();
            recordPage.SetBool("field", true);
            recordPage.Close();
            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            page.GetInt(0, out var isUsed);
            page.GetBool(4, out var value);

            Assert.IsTrue(canInsert);
            Assert.AreEqual(1, isUsed);
            Assert.AreEqual(true, value);
        }

        [Test]
        public void CanInsertRecordWithBlob()
        {
            var fn = RandomFilename;
            var block = new Block(fn, 0);

            var sch = new Schema();
            sch.AddBlobField("field", 50);

            recordPage = new RecordPage(block, _tableInfo, _transaction, _fileManager);
            var canInsert = recordPage.Insert();
            recordPage.SetBlob("field", new byte[] { 1, 2, 3 });
            recordPage.Close();
            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            page.GetInt(0, out var isUsed);
            page.GetBlob(4, out var value);

            Assert.IsTrue(canInsert);
            Assert.AreEqual(1, isUsed);
            Assert.AreEqual(new byte[] { 1, 2, 3 }, value);
        }

        [Test]
        public void CanInsertRecordWithString()
        {
            var fn = RandomFilename;
            var block = new Block(fn, 0);

            var sch = new Schema();
            sch.AddStringField("field", 50);

            recordPage = new RecordPage(block, _tableInfo, _transaction, _fileManager);
            var canInsert = recordPage.Insert();
            recordPage.SetString("field", "123123");
            recordPage.Close();
            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            page.GetInt(0, out var isUsed);
            page.GetString(4, out var value);

            Assert.IsTrue(canInsert);
            Assert.AreEqual(1, isUsed);
            Assert.AreEqual("123123", value);
        }

        [Test]
        public void CanInsertRecordWithDate()
        {
            var fn = RandomFilename;
            var block = new Block(fn, 0);

            var sch = new Schema();
            sch.AddDateField("field");

            recordPage = new RecordPage(block, _tableInfo, _transaction, _fileManager);
            var canInsert = recordPage.Insert();
            recordPage.SetDate("field", new DateTime(2020, 1, 1));
            recordPage.Close();
            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            page.GetInt(0, out var isUsed);
            page.GetDate(4, out var value);

            Assert.IsTrue(canInsert);
            Assert.AreEqual(1, isUsed);
            Assert.AreEqual(new DateTime(2020, 1, 1), value);
        }

        [Test]
        public void CanReadIntFromRecord()
        {
            var fn = RandomFilename;
            var block = new Block(fn, 0);

            var sch = new Schema();
            sch.AddIntField("field");

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            page.SetInt(0, 4);
            page.SetInt(4, 10);

            page.Write(block);

            recordPage = new RecordPage(block, _tableInfo, _transaction, _fileManager);
            recordPage.MoveToId(0);
            var value = recordPage.GetInt("field");

            Assert.AreEqual(10, value);
        }

        [Test]
        public void CanReadByteFromRecord()
        {
            var fn = RandomFilename;
            var block = new Block(fn, 0);

            var sch = new Schema();
            sch.AddByteField("field");

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            page.SetInt(0, 4);
            page.SetByte(4, 10);

            page.Write(block);

            recordPage = new RecordPage(block, _tableInfo, _transaction, _fileManager);
            recordPage.MoveToId(0);
            var value = recordPage.GetByte("field");

            Assert.AreEqual(10, value);
        }

        [Test]
        public void CanReadBoolFromRecord()
        {
            var fn = RandomFilename;
            var block = new Block(fn, 0);

            var sch = new Schema();
            sch.AddBoolField("field");

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            page.SetInt(0, 4);
            page.SetBool(4, true);

            page.Write(block);

            recordPage = new RecordPage(block, _tableInfo, _transaction, _fileManager);
            recordPage.MoveToId(0);
            var value = recordPage.GetBool("field");

            Assert.AreEqual(true, value);
        }

        [Test]
        public void CanReadBlobFromRecord()
        {
            var fn = RandomFilename;
            var block = new Block(fn, 0);

            var sch = new Schema();
            sch.AddBlobField("field", 30);

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            page.SetInt(0, 4);
            page.SetBlob(4, new byte[] { 1, 2, 3 });

            page.Write(block);

            recordPage = new RecordPage(block, _tableInfo, _transaction, _fileManager);
            recordPage.MoveToId(0);
            var value = recordPage.GetBlob("field");

            Assert.AreEqual(new byte[] { 1, 2, 3 }, value);
        }

        [Test]
        public void CanReadStringFromRecord()
        {
            var fn = RandomFilename;
            var block = new Block(fn, 0);

            var sch = new Schema();
            sch.AddStringField("field", 30);

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            page.SetInt(0, 4);
            page.SetString(4, "123");

            page.Write(block);

            recordPage = new RecordPage(block, _tableInfo, _transaction, _fileManager);
            recordPage.MoveToId(0);
            var value = recordPage.GetString("field");

            Assert.AreEqual("123", value);
        }

        [Test]
        public void CanReadDateFromRecord()
        {
            var fn = RandomFilename;
            var block = new Block(fn, 0);

            var sch = new Schema();
            sch.AddDateField("field");

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            page.SetInt(0, 4);
            page.SetDate(4, new DateTime(2020, 1, 1));

            page.Write(block);

            recordPage = new RecordPage(block, _tableInfo, _transaction, _fileManager);
            recordPage.MoveToId(0);
            var value = recordPage.GetDate("field");

            Assert.AreEqual(new DateTime(2020, 1, 1), value);
        }

        [Test]
        public void CanDeleteRecord()
        {
            var fn = RandomFilename;
            var block = new Block(fn, 0);

            recordPage = new RecordPage(block, _tableInfo, _transaction, _fileManager);
            var canInsert = recordPage.Insert();
            recordPage.SetInt("field", 20);
            recordPage.Delete();
            recordPage.Close();
            _transaction.Commit();

            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            page.GetInt(0, out var isUsed);
            page.GetInt(4, out var value);

            Assert.IsTrue(canInsert);
            Assert.AreEqual(0, isUsed);

            // It is "soft delete" - we are not changing the value
            Assert.AreEqual(20, value);
        }

        [OneTimeTearDown]
        public void ClearDirectory()
        {
            Directory.Delete("DBs", true);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
