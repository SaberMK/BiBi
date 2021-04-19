using BB.IO;
using BB.IO.Abstract;
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
    public class RecordFileTests
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

        private RecordFile _recordFile;

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
        }

        [Test]
        public void CanCreateRecordFile()
        {
            _tableInfo = new TableInfo(RandomFilename, new Schema());

            Assert.DoesNotThrow(() =>
            {
                _recordFile = new RecordFile(_tableInfo, _transaction);
            });
        }

        [Test]
        public void CanCreateAndCloseRecordFile()
        {
            _tableInfo = new TableInfo(RandomFilename, new Schema());

            Assert.DoesNotThrow(() =>
            {
                _recordFile = new RecordFile(_tableInfo, _transaction);
                _recordFile.Close();
            });
        }

        [Test]
        public void CanWriteIntOnARecord()
        {
            var tableFile = RandomFilename;
            var schema = new Schema();
            schema.AddIntField("field");
            _tableInfo = new TableInfo(tableFile, schema);

            _recordFile = new RecordFile(_tableInfo, _transaction);
            _recordFile.MoveToRID(new RID(0, 0));
            _recordFile.SetInt("field", 10);
            _recordFile.Close();

            _transaction.Commit();

            var block = new IO.Primitives.Block(tableFile+".tbl", 0);
            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            _ = page.GetInt(4, out var value);
            Assert.AreEqual(10, value);
        }

        [Test]
        public void CanWriteBoolOnARecord()
        {
            var tableFile = RandomFilename;
            var schema = new Schema();
            schema.AddBoolField("field");
            _tableInfo = new TableInfo(tableFile, schema);

            _recordFile = new RecordFile(_tableInfo, _transaction);
            _recordFile.MoveToRID(new RID(0, 0));
            _recordFile.SetBool("field", true);
            _recordFile.Close();

            _transaction.Commit();

            var block = new IO.Primitives.Block(tableFile + ".tbl", 0);
            var page = _fileManager.ResolvePage(block);
            page.Read(block);


            _ = page.GetBool(4, out var value);
            Assert.AreEqual(true, value);
        }

        [Test]
        public void CanWriteByteOnARecord()
        {
            var tableFile = RandomFilename;
            var schema = new Schema();
            schema.AddBoolField("field");
            _tableInfo = new TableInfo(tableFile, schema);

            _recordFile = new RecordFile(_tableInfo, _transaction);
            _recordFile.MoveToRID(new RID(0, 0));
            _recordFile.SetByte("field", 10);
            _recordFile.Close();

            _transaction.Commit();

            var block = new IO.Primitives.Block(tableFile + ".tbl", 0);
            var page = _fileManager.ResolvePage(block);
            page.Read(block);


            _ = page.GetByte(4, out var value);
            Assert.AreEqual(10, value);
        }

        [Test]
        public void CanWriteBlobOnARecord()
        {
            var tableFile = RandomFilename;
            var schema = new Schema();
            schema.AddBlobField("field", 50);
            _tableInfo = new TableInfo(tableFile, schema);

            _recordFile = new RecordFile(_tableInfo, _transaction);
            _recordFile.MoveToRID(new RID(0, 0));
            _recordFile.SetBlob("field", new byte[] { 10, 20 });
            _recordFile.Close();

            _transaction.Commit();

            var block = new IO.Primitives.Block(tableFile + ".tbl", 0);
            var page = _fileManager.ResolvePage(block);
            page.Read(block);


            _ = page.GetBlob(4, out var value);
            Assert.AreEqual(new byte[] { 10, 20 }, value);
        }

        [Test]
        public void CanWriteStringOnARecord()
        {
            var tableFile = RandomFilename;
            var schema = new Schema();
            schema.AddStringField("field", 50);
            _tableInfo = new TableInfo(tableFile, schema);

            _recordFile = new RecordFile(_tableInfo, _transaction);
            _recordFile.MoveToRID(new RID(0, 0));
            _recordFile.SetString("field", "temporary");
            _recordFile.Close();

            _transaction.Commit();

            var block = new IO.Primitives.Block(tableFile + ".tbl", 0);
            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            _ = page.GetString(4, out var value);
            Assert.AreEqual("temporary", value);
        }

        [Test]
        public void CanWriteDateOnARecord()
        {
            var tableFile = RandomFilename;
            var schema = new Schema();
            schema.AddDateField("field");
            _tableInfo = new TableInfo(tableFile, schema);

            _recordFile = new RecordFile(_tableInfo, _transaction);
            _recordFile.MoveToRID(new RID(0, 0));
            _recordFile.SetDate("field", new DateTime(2020, 1, 1));
            _recordFile.Close();

            _transaction.Commit();

            var block = new IO.Primitives.Block(tableFile + ".tbl", 0);
            var page = _fileManager.ResolvePage(block);
            page.Read(block);

            _ = page.GetDate(4, out var value);
            Assert.AreEqual(new DateTime(2020, 1, 1), value);
        }

        [Test]
        public void CanReadWrittenIntRecord()
        {
            var tableFile = RandomFilename;
            var schema = new Schema();
            schema.AddIntField("field");
            _tableInfo = new TableInfo(tableFile, schema);

            _recordFile = new RecordFile(_tableInfo, _transaction);
            _recordFile.MoveToRID(new RID(0, 0));
            _recordFile.SetInt("field", 10);
            _recordFile.Close();

            _transaction.Commit();

            var cm = new ConcurrencyManager();
            var newTr = new Transaction(_dispatcher, _bufferManager, cm, _fileManager, _logManager);

            var rf = new RecordFile(_tableInfo, newTr);
            rf.MoveToRID(new RID(0, 0));
            var value = rf.GetInt("field");

            Assert.AreEqual(10, value);
        }

        [Test]
        public void CanReadWrittenByteRecord()
        {
            var tableFile = RandomFilename;
            var schema = new Schema();
            schema.AddByteField("field");
            _tableInfo = new TableInfo(tableFile, schema);

            _recordFile = new RecordFile(_tableInfo, _transaction);
            _recordFile.MoveToRID(new RID(0, 0));
            _recordFile.SetByte("field", 10);
            _recordFile.Close();

            _transaction.Commit();

            var cm = new ConcurrencyManager();
            var newTr = new Transaction(_dispatcher, _bufferManager, cm, _fileManager, _logManager);

            var rf = new RecordFile(_tableInfo, newTr);
            rf.MoveToRID(new RID(0, 0));
            var value = rf.GetByte("field");

            Assert.AreEqual(10, value);
        }

        [Test]
        public void CanReadWrittenBoolRecord()
        {
            var tableFile = RandomFilename;
            var schema = new Schema();
            schema.AddBoolField("field");
            _tableInfo = new TableInfo(tableFile, schema);

            _recordFile = new RecordFile(_tableInfo, _transaction);
            _recordFile.MoveToRID(new RID(0, 0));
            _recordFile.SetBool("field", true);
            _recordFile.Close();

            _transaction.Commit();

            var cm = new ConcurrencyManager();
            var newTr = new Transaction(_dispatcher, _bufferManager, cm, _fileManager, _logManager);

            var rf = new RecordFile(_tableInfo, newTr);
            rf.MoveToRID(new RID(0, 0));
            var value = rf.GetBool("field");

            Assert.AreEqual(true, value);
        }

        [Test]
        public void CanReadWrittenBlobRecord()
        {
            var tableFile = RandomFilename;
            var schema = new Schema();
            schema.AddBlobField("field", 40);
            _tableInfo = new TableInfo(tableFile, schema);

            _recordFile = new RecordFile(_tableInfo, _transaction);
            _recordFile.MoveToRID(new RID(0, 0));
            _recordFile.SetBlob("field", new byte[] { 1, 2, 3, 4, 5 });
            _recordFile.Close();

            _transaction.Commit();

            var cm = new ConcurrencyManager();
            var newTr = new Transaction(_dispatcher, _bufferManager, cm, _fileManager, _logManager);

            var rf = new RecordFile(_tableInfo, newTr);
            rf.MoveToRID(new RID(0, 0));
            var value = rf.GetBlob("field");

            Assert.AreEqual(new byte[] { 1, 2, 3, 4, 5 }, value);
        }

        [Test]
        public void CanReadWrittenStringRecord()
        {
            var tableFile = RandomFilename;
            var schema = new Schema();
            schema.AddStringField("field", 40);
            _tableInfo = new TableInfo(tableFile, schema);

            _recordFile = new RecordFile(_tableInfo, _transaction);
            _recordFile.MoveToRID(new RID(0, 0));
            _recordFile.SetString("field", "huge string lol");
            _recordFile.Close();

            _transaction.Commit();

            var cm = new ConcurrencyManager();
            var newTr = new Transaction(_dispatcher, _bufferManager, cm, _fileManager, _logManager);

            var rf = new RecordFile(_tableInfo, newTr);
            rf.MoveToRID(new RID(0, 0));
            var value = rf.GetString("field");

            Assert.AreEqual("huge string lol", value);
        }

        [Test]
        public void CanReadWrittenDateRecord()
        {
            var tableFile = RandomFilename;
            var schema = new Schema();
            schema.AddDateField("field");
            _tableInfo = new TableInfo(tableFile, schema);

            _recordFile = new RecordFile(_tableInfo, _transaction);
            _recordFile.MoveToRID(new RID(0, 0));
            _recordFile.SetDate("field", new DateTime(2020, 1, 1));
            _recordFile.Close();

            _transaction.Commit();

            var cm = new ConcurrencyManager();
            var newTr = new Transaction(_dispatcher, _bufferManager, cm, _fileManager, _logManager);

            var rf = new RecordFile(_tableInfo, newTr);
            rf.MoveToRID(new RID(0, 0));
            var value = rf.GetDate("field");

            Assert.AreEqual(new DateTime(2020, 1, 1), value);
        }

        [Test]
        public void CanPlaceAndReadACoupleOfRecords()
        {
            var tableFile = RandomFilename;
            var schema = new Schema();
            schema.AddIntField("field");
            _tableInfo = new TableInfo(tableFile, schema);

            _recordFile = new RecordFile(_tableInfo, _transaction);
            _recordFile.MoveToRID(new RID(0, 0));
            _recordFile.SetInt("field", 10);

            //TODO think about it
            _recordFile.Insert();
            _recordFile.SetInt("field", 20);
            _recordFile.Close();

            _transaction.Commit();

            var cm = new ConcurrencyManager();
            var newTr = new Transaction(_dispatcher, _bufferManager, cm, _fileManager, _logManager);

            var rf = new RecordFile(_tableInfo, newTr);
            rf.MoveToRID(new RID(0, 0));
            var value = rf.GetInt("field");
            rf.Next();
            var value2 = rf.GetInt("field");

            Assert.AreEqual(10, value);
            Assert.AreEqual(20, value2);
        }

        [OneTimeTearDown]
        public void ClearDirectory()
        {
            Directory.Delete("DBs", true);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
