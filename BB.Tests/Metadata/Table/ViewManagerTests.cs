using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Metadata.Table;
using BB.Metadata.View;
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
    public class ViewManagerTests
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

        private TableManager tableManager;
        private ViewManager viewManager;

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

            var tableMetadataStorageFilename = RandomFilename;
            var fieldMetadataStorageFilename = RandomFilename;

            tableManager = new TableManager(true, _transaction, tableMetadataStorageFilename, fieldMetadataStorageFilename);
        }

        [Test]
        public void CanCreateViewManager()
        {
            var viewMetadataStorageFilename = RandomFilename;

            Assert.DoesNotThrow(() =>
            {
                viewManager = new ViewManager(true, tableManager, _transaction, viewMetadataStorageFilename);
            });


            var schema = new Schema();
            schema.AddStringField("viewname", ViewManager.MAX_VIEW_LENGTH);
            schema.AddStringField("viewdef", ViewManager.MAX_VIEW_LENGTH);

            var tableInfo = tableManager.GetTableInfo(viewMetadataStorageFilename, _transaction);

            Assert.AreEqual(viewMetadataStorageFilename + ".tbl", tableInfo.Filename);
            Assert.AreEqual(2, tableInfo.Schema.Fields.Count);

            var field1 = tableInfo.Schema.Fields["viewname"];
            var field2 = tableInfo.Schema.Fields["viewdef"];

            Assert.AreEqual(100, field1.Length);
            Assert.AreEqual(FieldType.String, field1.Type);

            Assert.AreEqual(100, field2.Length);
            Assert.AreEqual(FieldType.String, field2.Type);
        }

        [Test]
        public void CanCreateViewDefinition()
        {
            var viewMetadataStorageFilename = RandomFilename;

            viewManager = new ViewManager(true, tableManager, _transaction, viewMetadataStorageFilename);

            viewManager.CreateView("view1", "viewDefinition1", _transaction);

            _transaction.Commit();

            var schema = new Schema();
            schema.AddStringField("viewname", ViewManager.MAX_VIEW_LENGTH);
            schema.AddStringField("viewdef", ViewManager.MAX_VIEW_LENGTH);

            var viewTableInfo = tableManager.GetTableInfo(viewMetadataStorageFilename, _transaction);
            var viewTableCatalogRecordPage = new RecordFile(viewTableInfo, _transaction);

            viewTableCatalogRecordPage.BeforeFirst();
            viewTableCatalogRecordPage.Next();

            var viewName = viewTableCatalogRecordPage.GetString("viewname");
            var viewDefinition = viewTableCatalogRecordPage.GetString("viewdef");

            Assert.AreEqual("view1", viewName);
            Assert.AreEqual("viewDefinition1", viewDefinition);
        }

        [Test]
        public void CanCreateViewDefinitionInDifferentTransaction()
        {
            var viewMetadataStorageFilename = RandomFilename;

            viewManager = new ViewManager(true, tableManager, _transaction, viewMetadataStorageFilename);

            viewManager.CreateView("view1", "viewDefinition1", _transaction);

            _transaction.Commit();

            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            var schema = new Schema();
            schema.AddStringField("viewname", ViewManager.MAX_VIEW_LENGTH);
            schema.AddStringField("viewdef", ViewManager.MAX_VIEW_LENGTH);

            var viewTableInfo = tableManager.GetTableInfo(viewMetadataStorageFilename, _transaction);
            var viewTableCatalogRecordPage = new RecordFile(viewTableInfo, _transaction);

            viewTableCatalogRecordPage.BeforeFirst();
            viewTableCatalogRecordPage.Next();

            var viewName = viewTableCatalogRecordPage.GetString("viewname");
            var viewDefinition = viewTableCatalogRecordPage.GetString("viewdef");

            Assert.AreEqual("view1", viewName);
            Assert.AreEqual("viewDefinition1", viewDefinition);
        }

        [Test]
        public void CanCreateViewDefinitionBeforeItWasCommited()
        {
            var viewMetadataStorageFilename = RandomFilename;

            viewManager = new ViewManager(true, tableManager, _transaction, viewMetadataStorageFilename);

            viewManager.CreateView("view1", "viewDefinition1", _transaction);

            var schema = new Schema();
            schema.AddStringField("viewname", ViewManager.MAX_VIEW_LENGTH);
            schema.AddStringField("viewdef", ViewManager.MAX_VIEW_LENGTH);

            var viewTableInfo = tableManager.GetTableInfo(viewMetadataStorageFilename, _transaction);
            var viewTableCatalogRecordPage = new RecordFile(viewTableInfo, _transaction);

            viewTableCatalogRecordPage.BeforeFirst();
            viewTableCatalogRecordPage.Next();

            var viewName = viewTableCatalogRecordPage.GetString("viewname");
            var viewDefinition = viewTableCatalogRecordPage.GetString("viewdef");

            Assert.AreEqual("view1", viewName);
            Assert.AreEqual("viewDefinition1", viewDefinition);
        }

        [Test]
        public void CanGetViewDefinitionByName()
        {
            var viewMetadataStorageFilename = RandomFilename;

            viewManager = new ViewManager(true, tableManager, _transaction, viewMetadataStorageFilename);

            viewManager.CreateView("view1", "viewDefinition1", _transaction);

            var viewDefinition = viewManager.GetViewDefinition("view1", _transaction);

            _transaction.Commit();

            Assert.AreEqual("viewDefinition1", viewDefinition);
        }

        [Test]
        public void CanGetViewDefinitionInOtherTransaction()
        {
            var viewMetadataStorageFilename = RandomFilename;

            viewManager = new ViewManager(true, tableManager, _transaction, viewMetadataStorageFilename);

            viewManager.CreateView("view1", "viewDefinition1", _transaction);

            _transaction.Commit();
            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            var viewDefinition = viewManager.GetViewDefinition("view1", _transaction);

            Assert.AreEqual("viewDefinition1", viewDefinition);
        }


        [Test]
        public void CanGetViewDefinitionInDifferentViewManager()
        {
            var viewMetadataStorageFilename = RandomFilename;

            viewManager = new ViewManager(true, tableManager, _transaction, viewMetadataStorageFilename);

            viewManager.CreateView("view1", "viewDefinition1", _transaction);

            _transaction.Commit();
            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            viewManager = new ViewManager(false, tableManager, _transaction, viewMetadataStorageFilename);
            var viewDefinition = viewManager.GetViewDefinition("view1", _transaction);

            Assert.AreEqual("viewDefinition1", viewDefinition);
        }

        [Test]
        public void WouldGetEmptyStringIfViewWasNotFound()
        {
            var viewMetadataStorageFilename = RandomFilename;

            viewManager = new ViewManager(true, tableManager, _transaction, viewMetadataStorageFilename);

            viewManager.CreateView("view1", "viewDefinition1", _transaction);

            _transaction.Commit();
            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            viewManager = new ViewManager(false, tableManager, _transaction, viewMetadataStorageFilename);

            var viewDefinition = viewManager.GetViewDefinition("view2", _transaction);

            Assert.AreEqual(string.Empty, viewDefinition);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
