using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Metadata.Abstract;
using BB.Metadata.Indexes;
using BB.Metadata.Indexes.Implementations;
using BB.Metadata.Statistic;
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

namespace BB.Tests.Metadata.Indexes
{
    [TestFixture]
    public class IndexInfoTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private IBufferManager _bufferManager;
        private ITransactionNumberDispatcher _dispatcher;
        private IConcurrencyManager _concurrencyManager;

        private string _logName;
        private Transaction _transaction;

        private TableManager tableManager;
        private ViewManager viewManager;

        private TableInfo tableInfo;
        private string tableCatalogName;
        private string fieldCatalogName;
        private string viewCatalogName;

        private string tableName;

        private IndexInfo indexInfo;
        private string indexName;

        private StatisticsManager statisticsManager;

        [SetUp]
        public void Setup()
        {
            _logName = RandomFilename;
            _fileManager = new FileManager("temp", "DBs", 1024);
            _logManager = new LogManager(_fileManager, _logName);
            _bufferManager = new BufferManager(_fileManager, _logManager, new NaiveBufferPoolStrategy(_logManager, _fileManager, 1000));
            _dispatcher = new TransactionNumberDispatcher(10);
            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);

            var schema = new Schema();
            schema.AddIntField("Id");

            tableCatalogName = RandomFilename;
            fieldCatalogName = RandomFilename;
            viewCatalogName = RandomFilename;

            tableName = RandomFilename;

            tableManager = new TableManager(true, _transaction, tableCatalogName, fieldCatalogName);

            tableInfo = new TableInfo(tableName, schema);
            tableManager.CreateTable(tableName, schema, _transaction);

            viewManager = new ViewManager(true, tableManager, _transaction, viewCatalogName);
            statisticsManager = new StatisticsManager(tableManager, _transaction, tableCatalogName, fieldCatalogName, 0);
        }

        [Test]
        public void CanCreateIndexInfo()
        {
            Assert.DoesNotThrow(() =>
            {
                indexInfo = new IndexInfo("index", tableName, "Id", tableManager, statisticsManager, _transaction, 1024);
            });
        }

        [Test]
        public void CanOpenBaseIndex()
        {
            indexInfo = new IndexInfo("index", tableName, "Id", tableManager, statisticsManager, _transaction, 1024);

            BaseIndex index = null;

            Assert.DoesNotThrow(() =>
            {
                index = indexInfo.Open();
            });

            Assert.IsNotNull(index);
            Assert.IsTrue(index is BTreeIndex);
        }

        
        // I think I would change it later... for sure
        [Test]
        public void CanGetBlocksAccessedOnLessThanOneBlock()
        {
            var recordFile = new RecordFile(tableInfo, _transaction);
            recordFile.BeforeFirst();

            for(int i = 0; i < 10; ++i)
            {
                recordFile.Insert();
                recordFile.SetInt("Id", i * i);
            }
            recordFile.Close();
            _transaction.Commit();
            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);


            indexInfo = new IndexInfo("index", tableName, "Id", tableManager, statisticsManager, _transaction, 1024);
            
            var blocksAccessed = indexInfo.BlocksAccessed();

            Assert.AreEqual(0, blocksAccessed);
        }

        [Test]
        public void CanGetBlocksAccessedOnMoreThanOneBlock()
        {
            var recordFile = new RecordFile(tableInfo, _transaction);
            recordFile.BeforeFirst();

            for (int i = 0; i < 1024; ++i)
            {
                recordFile.Insert();
                recordFile.SetInt("Id", i * i);
            }
            recordFile.Close();
            _transaction.Commit();
            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);


            indexInfo = new IndexInfo("index", tableName, "Id", tableManager, statisticsManager, _transaction, 1024);

            var blocksAccessed = indexInfo.BlocksAccessed();

            Assert.AreEqual(0, blocksAccessed);
        }

        [Test]
        public void CanGetRecordsOutputOnLessThanOneBlock()
        {
            var recordFile = new RecordFile(tableInfo, _transaction);
            recordFile.BeforeFirst();

            for (int i = 0; i < 10; ++i)
            {
                recordFile.Insert();
                recordFile.SetInt("Id", i * i);
            }
            recordFile.Close();
            _transaction.Commit();
            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);


            indexInfo = new IndexInfo("index", tableName, "Id", tableManager, statisticsManager, _transaction, 1024);

            var recordsOutput = indexInfo.RecordsOutput();

            Assert.AreEqual(2, recordsOutput);
        }

        [Test]
        public void CanGetRecordsOutputOnMoreThanOneBlock()
        {
            var recordFile = new RecordFile(tableInfo, _transaction);
            recordFile.BeforeFirst();

            for (int i = 0; i < 1024; ++i)
            {
                recordFile.Insert();
                recordFile.SetInt("Id", i * i);
            }
            recordFile.Close();
            _transaction.Commit();
            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);


            indexInfo = new IndexInfo("index", tableName, "Id", tableManager, statisticsManager, _transaction, 1024);

            var recordsOutput = indexInfo.RecordsOutput();

            Assert.AreEqual(2, recordsOutput);
        }

        [Test]
        public void CanCreateIntegerIndex()
        {
            var indexTableName = RandomFilename;

            var schema = new Schema();
            schema.AddIntField("Field");

            tableInfo = new TableInfo(indexTableName, schema);
            tableManager.CreateTable(indexTableName, schema, _transaction);


            indexInfo = new IndexInfo("index", indexTableName, "Field", tableManager, statisticsManager, _transaction, 1024);

            // Todo check them out
            var indexSchema  = indexInfo.Open();

            Assert.Pass();
        }

        [Test]
        public void CanCreateByteIndex()
        {
            var indexTableName = RandomFilename;

            var schema = new Schema();
            schema.AddByteField("Field");

            tableInfo = new TableInfo(indexTableName, schema);
            tableManager.CreateTable(indexTableName, schema, _transaction);


            indexInfo = new IndexInfo("index", indexTableName, "Field", tableManager, statisticsManager, _transaction, 1024);

            // Todo check them out
            var indexSchema = indexInfo.Open();

            Assert.Pass();
        }

        [Test]
        public void CanCreateBoolIndex()
        {
            var indexTableName = RandomFilename;

            var schema = new Schema();
            schema.AddBoolField("Field");

            tableInfo = new TableInfo(indexTableName, schema);
            tableManager.CreateTable(indexTableName, schema, _transaction);


            indexInfo = new IndexInfo("index", indexTableName, "Field", tableManager, statisticsManager, _transaction, 1024);

            // Todo check them out
            var indexSchema = indexInfo.Open();

            Assert.Pass();
        }

        [Test]
        public void CanCreateDateIndex()
        {
            var indexTableName = RandomFilename;

            var schema = new Schema();
            schema.AddDateField("Field");

            tableInfo = new TableInfo(indexTableName, schema);
            tableManager.CreateTable(indexTableName, schema, _transaction);


            indexInfo = new IndexInfo("index", indexTableName, "Field", tableManager, statisticsManager, _transaction, 1024);

            // Todo check them out
            var indexSchema = indexInfo.Open();

            Assert.Pass();
        }

        [Test]
        public void CanCreateBlobIndex()
        {
            var indexTableName = RandomFilename;

            var schema = new Schema();
            schema.AddBlobField("Field", 30);

            tableInfo = new TableInfo(indexTableName, schema);
            tableManager.CreateTable(indexTableName, schema, _transaction);


            indexInfo = new IndexInfo("index", indexTableName, "Field", tableManager, statisticsManager, _transaction, 1024);

            // Todo check them out
            var indexSchema = indexInfo.Open();

            Assert.Pass();
        }

        [Test]
        public void CanCreateStringIndex()
        {
            var indexTableName = RandomFilename;

            var schema = new Schema();
            schema.AddStringField("Field", 50);

            tableInfo = new TableInfo(indexTableName, schema);
            tableManager.CreateTable(indexTableName, schema, _transaction);


            indexInfo = new IndexInfo("index", indexTableName, "Field", tableManager, statisticsManager, _transaction, 1024);

            // Todo check them out
            var indexSchema = indexInfo.Open();

            Assert.Pass();
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
