using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Metadata.Indexes.Implementations;
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

namespace BB.Tests.Metadata.Indexes.Implementations
{
    [TestFixture]
    public class BTreeIndexTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private IBufferManager _bufferManager;
        private ITransactionNumberDispatcher _dispatcher;
        private IConcurrencyManager _concurrencyManager;

        private string _logName;
        private Transaction _transaction;

        private TableManager tableManager;

        private string indexName;
        private Schema schema;
        private Transaction transaction;

        private BTreeIndex index;

        [SetUp]
        public void Setup()
        {
            indexName = "tempIndex";

            schema = new Schema();
            schema.AddIntField("id");

            _logName = RandomFilename;
            _fileManager = new FileManager("temp", "DBs", 1024);
            _logManager = new LogManager(_fileManager, _logName);
            _bufferManager = new BufferManager(_fileManager, _logManager, new NaiveBufferPoolStrategy(_logManager, _fileManager, 10));
            _dispatcher = new TransactionNumberDispatcher(10);
            _concurrencyManager = new ConcurrencyManager();
            transaction = new Transaction(_dispatcher, _bufferManager, _concurrencyManager, _fileManager, _logManager);
        }

        [Test]
        public void CanCreateBTreeIndex()
        {
            Assert.DoesNotThrow(() =>
            {
                index = new BTreeIndex("id", schema, transaction);
            });
        }

        [Test]
        public void CanCountSearchCost()
        {
            var res = 0;

            Assert.DoesNotThrow(() =>
            {
                var res = BTreeIndex.SearchCost(0, 0);
            });

            Assert.AreEqual(0, res);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
