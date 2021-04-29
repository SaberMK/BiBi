using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Metadata;
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

namespace BB.Tests.Metadata.Statistic
{
    [TestFixture]
    public class StatisticalManagerTests
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

        private StatisticsManager manager;

        private string tableCatalogName;
        private string fieldCatalogName; 

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

            tableManager = new TableManager(true, _transaction, tableCatalogName, fieldCatalogName);

            tableManager.CreateTable("table1", schema, _transaction);

            viewManager = new ViewManager(true, tableManager, _transaction);
            //var indexManager = new IndexManager(true, tableManager, )
            //var metadataManager = new MetadataManager(fileManager, tableManager, viewManager, inde)
        }

        [Test]
        public void CanCreateStatisticsManager()
        {
            Assert.DoesNotThrow(() =>
            {
                manager = new StatisticsManager(tableManager, _transaction, tableCatalogName, fieldCatalogName);
            });
        }

        [Test]
        public void CanGetStatisticsFromEmptyTable()
        {
            manager = new StatisticsManager(tableManager, _transaction, tableCatalogName, fieldCatalogName, 2);
            var data = manager.GetStatisticalInfo("table1", _transaction);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
