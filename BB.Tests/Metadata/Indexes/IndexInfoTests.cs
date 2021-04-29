using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Metadata.Indexes;
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
            _bufferManager = new BufferManager(_fileManager, _logManager, new NaiveBufferPoolStrategy(_logManager, _fileManager, 10));
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
            statisticsManager = new StatisticsManager(tableManager, _transaction, tableCatalogName, fieldCatalogName);

        }

        [Test]
        public void CanCreateIndexInfo()
        {
            Assert.DoesNotThrow(() =>
            {
                indexInfo = new IndexInfo("index", tableName, "Id", tableManager, statisticsManager, _transaction, 1024);
            });
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
