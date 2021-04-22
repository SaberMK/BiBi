using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Transactions.Abstract;
using BB.Transactions.Concurrency;
using BB.Transactions.Records;
using BB.Transactions.Recovery;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace BB.Tests.Transactions.Concurrency
{
    public class ConcurrencyManagerTests
    {
        private LockTable lockTable;
        private IConcurrencyManager concurrencyManager;

        [SetUp]
        public void Setup()
        {
            lockTable = new LockTable(null, null);
        }

        [Test]
        public void CanCreateConcurrencyManager()
        {
            Assert.DoesNotThrow(() =>
            {
                concurrencyManager = new ConcurrencyManager(lockTable);
            });
        }

        [Test]
        public void CanCreateDefaultConcurrencyManager()
        {
            Assert.DoesNotThrow(() =>
            {
                concurrencyManager = new ConcurrencyManager();
            });
        }

        [Test]
        public void CanTakeSharedLock()
        {
            concurrencyManager = new ConcurrencyManager();

            var block = new Block(RandomFilename, 0);

            Assert.DoesNotThrow(() =>
            {
                concurrencyManager.SharedLock(block);
            });
        }

        [Test]
        public void CanTakeMultipleSharedLocks()
        {
            concurrencyManager = new ConcurrencyManager();

            var block = new Block(RandomFilename, 0);

            Assert.DoesNotThrow(() =>
            {
                concurrencyManager.SharedLock(block);
                concurrencyManager.SharedLock(block);
                concurrencyManager.SharedLock(block);
                concurrencyManager.SharedLock(block);
            });
        }

        [Test]
        public void CanTakeExclusiveLock()
        {
            concurrencyManager = new ConcurrencyManager();

            var block = new Block(RandomFilename, 0);

            Assert.DoesNotThrow(() =>
            {
                concurrencyManager.ExclusiveLock(block);
            });
        }

        [Test]
        public void CanTakeExclusiveLockIfAlreadyTaken()
        {
            var lockTable = new LockTable(TimeSpan.FromMilliseconds(300), TimeSpan.FromMilliseconds(100));
            concurrencyManager = new ConcurrencyManager();

            var block = new Block(RandomFilename, 0);

            Assert.DoesNotThrow(() =>
            {
                concurrencyManager.ExclusiveLock(block);
                concurrencyManager.ExclusiveLock(block);
            });
        }

        [Test]
        public void CanTakeAndReleaseSharedLock()
        {
            concurrencyManager = new ConcurrencyManager();

            var block = new Block(RandomFilename, 0);

            Assert.DoesNotThrow(() =>
            {
                concurrencyManager.SharedLock(block);
                concurrencyManager.Release();
            });
        }

        [Test]
        public void CanTakeAndReleaseExclusiveLock()
        {
            concurrencyManager = new ConcurrencyManager();

            var block = new Block(RandomFilename, 0);

            Assert.DoesNotThrow(() =>
            {
                concurrencyManager.ExclusiveLock(block);
                concurrencyManager.Release();
            });
        }

        [Test]
        public void ReleaseMethodDoesNotThrowItNoLocksProvided()
        {
            concurrencyManager = new ConcurrencyManager();

            var block = new Block(RandomFilename, 0);

            Assert.DoesNotThrow(() =>
            {
                concurrencyManager.Release();
            });
        }

        [OneTimeTearDown]
        public void ClearDirectory()
        {
            Directory.Delete("DBs", true);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
