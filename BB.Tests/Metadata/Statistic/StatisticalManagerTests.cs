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

        private TableInfo tableInfo;
        private string _logName;
        private Transaction _transaction;

        private TableManager tableManager;
        private ViewManager viewManager;

        private StatisticsManager manager;

        private string tableCatalogName;
        private string fieldCatalogName;
        private string viewCatalogName;

        private string tableName;

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

            var data = manager.GetStatisticalInfo(tableName, _transaction);

            Assert.AreEqual(1, data.BlocksAccessed);
            Assert.AreEqual(0, data.RecordsOutput);

            // Well... this formula is not that accurate :D
            Assert.AreEqual(1, data.DistinctValues("Id"));
        }

        [Test]
        public void CanGetStatisticsFromTableFilledWithValues()
        {
            manager = new StatisticsManager(tableManager, _transaction, tableCatalogName, fieldCatalogName, 1);

            var data = manager.GetStatisticalInfo(tableName, _transaction);

            var recordFile = new RecordFile(tableInfo, _transaction);
            recordFile.BeforeFirst();
            
            for(int i = 0; i < 30; ++i)
            {
                recordFile.Insert();
                recordFile.SetInt("Id", i + 10);
            }
            _transaction.Commit();

            //Before update

            Assert.AreEqual(1, data.BlocksAccessed);
            Assert.AreEqual(0, data.RecordsOutput);

            Assert.AreEqual(1, data.DistinctValues("Id"));

            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);
            //After update
            var updatedData = manager.GetStatisticalInfo(tableName, _transaction);

            Assert.AreEqual(30, updatedData.RecordsOutput);
            Assert.AreEqual(1, updatedData.BlocksAccessed);
            Assert.AreEqual(11, updatedData.DistinctValues("Id"));
        }

        [Test]
        public void CanGetStatisticsFromTableFilledWithValuesInTheSameTransaction()
        {
            manager = new StatisticsManager(tableManager, _transaction, tableCatalogName, fieldCatalogName, 1);

            var data = manager.GetStatisticalInfo(tableName, _transaction);

            var recordFile = new RecordFile(tableInfo, _transaction);
            recordFile.BeforeFirst();

            for (int i = 0; i < 30; ++i)
            {
                recordFile.Insert();
                recordFile.SetInt("Id", i + 10);
            }

            //Before update

            Assert.AreEqual(1, data.BlocksAccessed);
            Assert.AreEqual(0, data.RecordsOutput);

            Assert.AreEqual(1, data.DistinctValues("Id"));

            //After update
            var updatedData = manager.GetStatisticalInfo(tableName, _transaction);

            Assert.AreEqual(30, updatedData.RecordsOutput);
            Assert.AreEqual(1, updatedData.BlocksAccessed);
            Assert.AreEqual(11, updatedData.DistinctValues("Id"));
        }

        [Test]
        public void CanGetStatisticsFromNewTable()
        {
            manager = new StatisticsManager(tableManager, _transaction, tableCatalogName, fieldCatalogName, 1);

            var data = manager.GetStatisticalInfo(tableName, _transaction);

            var newTableName = RandomFilename;

            var schema = new Schema();
            schema.AddIntField("Id");
            schema.AddStringField("Guid", 40);

            var newTableInfo = new TableInfo(newTableName, schema);
            tableManager.CreateTable(newTableName, schema, _transaction);

            var recordFile = new RecordFile(newTableInfo, _transaction);
            recordFile.BeforeFirst();

            for (int i = 0; i < 50; ++i)
            {
                recordFile.Insert();
                recordFile.SetInt("Id", i + 10);
                recordFile.SetString("Guid", Guid.NewGuid().ToString());
            }

            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);
            //Before update

            Assert.AreEqual(1, data.BlocksAccessed);
            Assert.AreEqual(0, data.RecordsOutput);

            Assert.AreEqual(1, data.DistinctValues("Id"));

            _concurrencyManager = new ConcurrencyManager();
            _transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);
            //After update
            var updatedData = manager.GetStatisticalInfo(newTableName, _transaction);

            Assert.AreEqual(50, updatedData.RecordsOutput);
            Assert.AreEqual(3, updatedData.BlocksAccessed);
            Assert.AreEqual(17, updatedData.DistinctValues("Id"));
        }

        [Test]
        public void CanGetStatisticsFromNewTableInTheSameTransaction()
        {
            manager = new StatisticsManager(tableManager, _transaction, tableCatalogName, fieldCatalogName, 1);


            var newTableName = RandomFilename;

            var schema = new Schema();
            schema.AddIntField("Id");
            schema.AddStringField("Guid", 40);

            var newTableInfo = new TableInfo(newTableName, schema);
            tableManager.CreateTable(newTableName, schema, _transaction);

            var recordFile = new RecordFile(newTableInfo, _transaction);
            recordFile.BeforeFirst();

            for (int i = 0; i < 50; ++i)
            {
                recordFile.Insert();
                recordFile.SetInt("Id", i + 10);
                recordFile.SetString("Guid", Guid.NewGuid().ToString());
            }

            var updatedData = manager.GetStatisticalInfo(newTableName, _transaction);

            Assert.AreEqual(50, updatedData.RecordsOutput);
            Assert.AreEqual(3, updatedData.BlocksAccessed);
            Assert.AreEqual(17, updatedData.DistinctValues("Id"));
        }

        [Test]
        public void WouldGetNullFromNotUnexistingTable()
        {
            manager = new StatisticsManager(tableManager, _transaction, tableCatalogName, fieldCatalogName, 1);

            var value = manager.GetStatisticalInfo(Guid.NewGuid().ToString(), _transaction);

            Assert.IsNull(value);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
