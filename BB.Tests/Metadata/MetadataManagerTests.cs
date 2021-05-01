using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Metadata;
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

namespace BB.Tests.Metadata
{
    [TestFixture]
    class MetadataManagerTests
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
        private string indexCatalogName;

        private string tableName;

        private IndexInfo indexInfo;
        private string indexName;

        private StatisticsManager statisticsManager;

        private IndexManager indexManager;

        private MetadataManager metadataManager;

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
            indexCatalogName = RandomFilename;

            tableName = RandomFilename;

            tableManager = new TableManager(true, _transaction, tableCatalogName, fieldCatalogName);

            tableInfo = new TableInfo(tableName, schema);
            tableManager.CreateTable(tableName, schema, _transaction);

            viewManager = new ViewManager(true, tableManager, _transaction, viewCatalogName);
            statisticsManager = new StatisticsManager(tableManager, _transaction, tableCatalogName, fieldCatalogName, 0);

            indexManager = new IndexManager(true, tableManager, statisticsManager, _transaction, indexCatalogName, _fileManager.BlockSize);
        }

        [Test]
        public void CanCreateMetadataManager()
        {
            Assert.DoesNotThrow(() =>
            {
                metadataManager = new MetadataManager(_fileManager, tableManager, viewManager, indexManager, statisticsManager);
            });
        }

        [Test]
        public void CanGetBlockSize()
        {
            metadataManager = new MetadataManager(_fileManager, tableManager, viewManager, indexManager, statisticsManager);
            var blockSize = metadataManager.BlockSize;

            Assert.AreEqual(1024, blockSize);
        }

        [Test]
        public void CanCreateIndexAndGetInfo()
        {
            metadataManager = new MetadataManager(_fileManager, tableManager, viewManager, indexManager, statisticsManager);

            Assert.DoesNotThrow(() =>
            {
                metadataManager.CreateIndex(RandomFilename, tableName, "Id", _transaction);
            });
            
            var index = metadataManager.GetIndexInfo(tableName, _transaction);

            Assert.IsNotNull(index);
            Assert.AreEqual(0, index["Id"].BlocksAccessed());
            Assert.AreEqual(0, index["Id"].RecordsOutput());
        }

        [Test]
        public void CanCreateIndexOnFilledTable()
        {
            var recordFile = new RecordFile(tableInfo, _transaction);

            recordFile.BeforeFirst();
            for(int i =0;i<10;++i)
            {
                recordFile.Insert();
                recordFile.SetInt("Id", i - 1);
            }
            recordFile.Close();

            metadataManager = new MetadataManager(_fileManager, tableManager, viewManager, indexManager, statisticsManager);

            Assert.DoesNotThrow(() =>
            {
                metadataManager.CreateIndex(RandomFilename, tableName, "Id", _transaction);
            });

            var index = metadataManager.GetIndexInfo(tableName, _transaction);

            Assert.IsNotNull(index);
            Assert.AreEqual(0, index["Id"].BlocksAccessed());
            Assert.AreEqual(2, index["Id"].RecordsOutput());
        }

        [Test]
        public void CanCreateTableAndGetTableInfo()
        {
            metadataManager = new MetadataManager(_fileManager, tableManager, viewManager, indexManager, statisticsManager);

            var schema = new Schema();
            schema.AddIntField("Id");
            schema.AddStringField("Name", 50);

            var tableName = RandomFilename;

            Assert.DoesNotThrow(() =>
            {
                metadataManager.CreateTable(tableName, schema, _transaction);
            });

            var tableInfo = metadataManager.GetTableInfo(tableName, _transaction);

            Assert.IsNotNull(tableInfo);
            Assert.AreEqual(58, tableInfo.RecordLength);
        }

        [Test]
        public void CanCreateViewAndGetViewInfo()
        {
            metadataManager = new MetadataManager(_fileManager, tableManager, viewManager, indexManager, statisticsManager);

            var schema = new Schema();
            schema.AddIntField("Id");
            schema.AddStringField("Name", 50);

            var tableName = RandomFilename;

            Assert.DoesNotThrow(() =>
            {
                metadataManager.CreateView(tableName, "definition of a view", _transaction);
            });

            var viewDefinition = metadataManager.GetViewDefinition(tableName, _transaction);

            Assert.IsNotNull(viewDefinition);
            Assert.AreEqual("definition of a view", viewDefinition);
        }

        [Test]
        public void CanGetStatisticsInfo()
        {
            metadataManager = new MetadataManager(_fileManager, tableManager, viewManager, indexManager, statisticsManager);

            StatisticalInfo info = null;

            Assert.DoesNotThrow(() =>
            {
                info = metadataManager.GetStatisticsInfo(tableName, _transaction);
            });

            Assert.IsNotNull(info);
            Assert.AreEqual(1, info.BlocksAccessed);
            Assert.AreEqual(0, info.RecordsOutput);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
