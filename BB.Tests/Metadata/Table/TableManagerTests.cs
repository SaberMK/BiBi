using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Metadata.Table;
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

namespace BB.Tests.Metadata.Table
{
    [TestFixture]
    public class TableManagerTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private IBufferManager _bufferManager;
        private ITransactionNumberDispatcher _dispatcher;
        private IConcurrencyManager _concurrencyManager;

        private string _logName;
        private Transaction _transaction;

        private TableManager tableManager;

        [SetUp]
        public void Setup()
        {
            _logName = RandomFilename;
            _fileManager = new FileManager("temp", "DBs", 1024);
            _logManager = new LogManager(_fileManager, _logName);
            _bufferManager = new BufferManager(_fileManager, _logManager, new NaiveBufferPoolStrategy(_logManager, _fileManager, 10));
            _dispatcher = new TransactionNumberDispatcher(10);
            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

        }

        [Test]
        public void CanCreateNewTableManager()
        {
            Assert.DoesNotThrow(() =>
            {
                tableManager = new TableManager(true, _transaction, RandomFilename, RandomFilename);
            });
        }

        [Test]
        public void CanCreateTable()
        {
            var tableMetadataStorageFilename = RandomFilename;
            var fieldMetadataStorageFilename = RandomFilename;

            tableManager = new TableManager(true, _transaction, tableMetadataStorageFilename, fieldMetadataStorageFilename);
            var schema = new Schema();

            schema.AddIntField("field1");
            schema.AddBlobField("field2", 40);

            tableManager.CreateTable("table1", schema, _transaction);

            _transaction.Commit();

            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            var tableCatalogSchema = new Schema();
            tableCatalogSchema.AddStringField("tblname", TableManager.MAX_NAME_LENGTH);
            tableCatalogSchema.AddIntField("reclength");

            var tableCatalogInfo = new TableInfo(tableMetadataStorageFilename, tableCatalogSchema);
            var tableCatalogRecordPage = new RecordFile(tableCatalogInfo, _transaction);

            tableCatalogRecordPage.BeforeFirst();
            tableCatalogRecordPage.Next();

            // skip all tables definition and all fields definition tables
            tableCatalogRecordPage.Next();
            tableCatalogRecordPage.Next();

            var tableName = tableCatalogRecordPage.GetString("tblname");
            var recordLength = tableCatalogRecordPage.GetInt("reclength");

            Assert.AreEqual("table1", tableName);
            Assert.AreEqual(new TableInfo("table1", schema).RecordLength, recordLength);
            //var tableMetadataBlock = new Block(tableMetadataStorageFilename + ".tbl", 0);
            //var tableMetadataPage = _fileManager.ResolvePage();
            //tableMetadataPage.Read(tableMetadataBlock);

            var fieldCatalogSchema = new Schema();
            fieldCatalogSchema.AddStringField("tblname", TableManager.MAX_NAME_LENGTH);
            fieldCatalogSchema.AddStringField("fldname", TableManager.MAX_NAME_LENGTH);
            fieldCatalogSchema.AddIntField("type");
            fieldCatalogSchema.AddIntField("length");
            fieldCatalogSchema.AddIntField("offset");
            var fieldCatalogInfo = new TableInfo(fieldMetadataStorageFilename, fieldCatalogSchema);
            var fieldCatalogRecordPage = new RecordFile(fieldCatalogInfo, _transaction);

            fieldCatalogRecordPage.BeforeFirst();
            fieldCatalogRecordPage.Next();

            // 2 field for table metadata table, 5 fields for field metadata table
            fieldCatalogRecordPage.Next();
            fieldCatalogRecordPage.Next();
            fieldCatalogRecordPage.Next();
            fieldCatalogRecordPage.Next();
            fieldCatalogRecordPage.Next();
            fieldCatalogRecordPage.Next();
            fieldCatalogRecordPage.Next();

            var field1Table = fieldCatalogRecordPage.GetString("tblname");
            var field1Name = fieldCatalogRecordPage.GetString("fldname");
            var field1Type = fieldCatalogRecordPage.GetInt("type");
            var field1Length = fieldCatalogRecordPage.GetInt("length");
            var field1Offset = fieldCatalogRecordPage.GetInt("offset");

            Assert.AreEqual("table1", field1Table);
            Assert.AreEqual("field1", field1Name);
            Assert.AreEqual(2, field1Type);
            Assert.AreEqual(sizeof(int), field1Length);
            Assert.AreEqual(0, field1Offset);

            fieldCatalogRecordPage.Next();

            var field2Table = fieldCatalogRecordPage.GetString("tblname");
            var field2Name = fieldCatalogRecordPage.GetString("fldname");
            var field2Type = fieldCatalogRecordPage.GetInt("type");
            var field2Length = fieldCatalogRecordPage.GetInt("length");
            var field2Offset = fieldCatalogRecordPage.GetInt("offset");

            Assert.AreEqual("table1", field2Table);
            Assert.AreEqual("field2", field2Name);
            Assert.AreEqual(4, field2Type);
            Assert.AreEqual(40, field2Length);
            Assert.AreEqual(4, field2Offset);
        }

        [Test]
        public void CanGetTableInfoWhileItWasUncommited()
        {
            var tableMetadataStorageFilename = RandomFilename;
            var fieldMetadataStorageFilename = RandomFilename;

            tableManager = new TableManager(true, _transaction, tableMetadataStorageFilename, fieldMetadataStorageFilename);
            var schema = new Schema();

            schema.AddIntField("field1");
            schema.AddBlobField("field2", 40);

            tableManager.CreateTable("table1", schema, _transaction);

            var tableInfo = tableManager.GetTableInfo("table1", _transaction);

            Assert.AreEqual("table1.tbl", tableInfo.Filename);
            Assert.AreEqual(2, tableInfo.Schema.Fields.Count);
            Assert.AreEqual(48, tableInfo.RecordLength);

            var field1 = tableInfo.Schema.Fields["field1"];
            var field2 = tableInfo.Schema.Fields["field2"];

            Assert.AreEqual(FieldType.Integer, field1.Type);
            Assert.AreEqual(FieldType.Blob, field2.Type);
            Assert.AreEqual(40, field2.Length);
        }

        [Test]
        public void CanGetTableInfoInDifferentTransaction()
        {
            var tableMetadataStorageFilename = RandomFilename;
            var fieldMetadataStorageFilename = RandomFilename;

            tableManager = new TableManager(true, _transaction, tableMetadataStorageFilename, fieldMetadataStorageFilename);
            var schema = new Schema();

            schema.AddIntField("field1");
            schema.AddBlobField("field2", 40);

            tableManager.CreateTable("table1", schema, _transaction);

            _transaction.Commit();

            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            var tableInfo = tableManager.GetTableInfo("table1", _transaction);

            Assert.AreEqual("table1.tbl", tableInfo.Filename);
            Assert.AreEqual(2, tableInfo.Schema.Fields.Count);
            Assert.AreEqual(48, tableInfo.RecordLength);

            var field1 = tableInfo.Schema.Fields["field1"];
            var field2 = tableInfo.Schema.Fields["field2"];

            Assert.AreEqual(FieldType.Integer, field1.Type);
            Assert.AreEqual(FieldType.Blob, field2.Type);
            Assert.AreEqual(40, field2.Length);
        }


        [Test]
        public void CanGetTableInfoInDifferentTableManager()
        {
            var tableMetadataStorageFilename = RandomFilename;
            var fieldMetadataStorageFilename = RandomFilename;

            tableManager = new TableManager(true, _transaction, tableMetadataStorageFilename, fieldMetadataStorageFilename);
            var schema = new Schema();

            schema.AddIntField("field1");
            schema.AddBlobField("field2", 40);

            tableManager.CreateTable("table1", schema, _transaction);

            _transaction.Commit();

            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            tableManager = new TableManager(false, _transaction, tableMetadataStorageFilename, fieldMetadataStorageFilename);

            var tableInfo = tableManager.GetTableInfo("table1", _transaction);

            Assert.AreEqual("table1.tbl", tableInfo.Filename);
            Assert.AreEqual(2, tableInfo.Schema.Fields.Count);
            Assert.AreEqual(48, tableInfo.RecordLength);

            var field1 = tableInfo.Schema.Fields["field1"];
            var field2 = tableInfo.Schema.Fields["field2"];

            Assert.AreEqual(FieldType.Integer, field1.Type);
            Assert.AreEqual(FieldType.Blob, field2.Type);
            Assert.AreEqual(40, field2.Length);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
