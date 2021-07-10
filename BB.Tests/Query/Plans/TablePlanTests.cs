using BB.IO;
using BB.IO.Abstract;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Metadata;
using BB.Metadata.Indexes;
using BB.Metadata.Statistic;
using BB.Metadata.Table;
using BB.Metadata.View;
using BB.Query.Plans;
using BB.Record.Base;
using BB.Record.Entity;
using BB.Transactions;
using BB.Transactions.Abstract;
using BB.Transactions.Concurrency;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Tests.Query.Plans
{
    [TestFixture]
    public class TablePlanTests
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

        private TablePlan tablePlan;

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

            metadataManager = new MetadataManager(_fileManager, tableManager, viewManager, indexManager, statisticsManager);
        }


        [Test]
        public void CanCreate()
        {
            Assert.DoesNotThrow(() =>
            {
                tablePlan = new TablePlan(tableName, metadataManager, _transaction);
            });
        }

        [Test]
        public void CanCreatePlanFromTableWithData()
        {
            var recordFile = new RecordFile(tableInfo, _transaction);
            recordFile.BeforeFirst();

            for (int i = 0; i < 30; ++i)
            {
                recordFile.Insert();
                recordFile.SetInt("Id", i + 10);
            }
            _transaction.Commit();

            tablePlan = new TablePlan(tableName, metadataManager, _transaction);

            Assert.DoesNotThrow(() =>
            {
                var scan = tablePlan.Open();
            });
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
