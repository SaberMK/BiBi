using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Logger;
using BB.Transactions.Concurrency;
using BB.Transactions.Exceptions;
using NUnit.Framework;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace BB.Tests.Transactions.Concurrency
{
    public class LockTableTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private LockTable _lockTable;

        [SetUp]
        public void Setup()
        {
            _fileManager = new FileManager("temp", "DBs", 100);
            _logManager = new LogManager(_fileManager, RandomFilename);
        }

        [Test]
        public void CanCreateLockTable()
        {
            Assert.DoesNotThrow(() =>
            {
                _lockTable = new LockTable(TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(300));
            });
        }

        [Test]
        public void CanCreateDefaultLockTable()
        {
            Assert.DoesNotThrow(() =>
            {
                _lockTable = new LockTable(null, null);
            });
        }

        [Test]
        public void CanSetSharedLock()
        {
            _lockTable = new LockTable(TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(300));

            Assert.DoesNotThrow(() =>
            {
                _lockTable.SharedLock(new Block(RandomFilename, 0));
            });
        }


        [Test]
        public void CanSetExclusiveLock()
        {
            _lockTable = new LockTable(TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(300));

            Assert.DoesNotThrow(() =>
            {
                var block = new Block(RandomFilename, 0);
                _lockTable.ExclusiveLock(block);
            });
        }

        [Test]
        public void CanUnlockSharedLock()
        {
            _lockTable = new LockTable(TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(300));

            Assert.DoesNotThrow(() =>
            {
                var block = new Block(RandomFilename, 0);
                _lockTable.SharedLock(block);
                _lockTable.Unlock(block);
            });
        }

        [Test]
        public void CanUnlockExclusiveLock()
        {
            _lockTable = new LockTable(TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(300));

            Assert.DoesNotThrow(() =>
            {
                var block = new Block(RandomFilename, 0);
                _lockTable.ExclusiveLock(block);
                _lockTable.Unlock(block);
            });
        }

        [Test]
        public void CanHaveMultipleSharedLocks()
        {
            _lockTable = new LockTable(TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(300));

            Assert.DoesNotThrow(() =>
            {
                var block = new Block(RandomFilename, 0);
                _lockTable.SharedLock(block);
                _lockTable.SharedLock(block);
                _lockTable.SharedLock(block);
                _lockTable.Unlock(block);
            });
        }

        [Test]
        public void CanWaitForSharedLockIfHaveExclusive()
        {
            _lockTable = new LockTable(TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(100));

            Assert.DoesNotThrow(() =>
            {
                var block = new Block(RandomFilename, 0);
                
                _lockTable.ExclusiveLock(block);

                Task.Run(async () =>
                {
                    await Task.Delay(500);
                    _lockTable.Unlock(block);
                });

                _lockTable.SharedLock(block);
                _lockTable.Unlock(block);
            });
        }

        [Test]
        public void CanWaitForExclusiveLockIfHaveExclusive()
        {
            _lockTable = new LockTable(TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(100));

            Assert.DoesNotThrow(() =>
            {
                var block = new Block(RandomFilename, 0);

                _lockTable.ExclusiveLock(block);

                Task.Run(async () =>
                {
                    await Task.Delay(500);
                    _lockTable.Unlock(block);
                });

                _lockTable.ExclusiveLock(block);
                _lockTable.Unlock(block);
            });
        }

        [Test]
        public void CannotWaitForExclusiveLockIfHaveExclusive()
        {
            _lockTable = new LockTable(TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(100));

            Assert.Throws<LockAbortException>(() =>
            {
                var block = new Block(RandomFilename, 0);

                _lockTable.ExclusiveLock(block);
                _lockTable.ExclusiveLock(block);
                _lockTable.Unlock(block);
            });
        }

        [Test]
        public void NotThrowsExceptionIfExclusiveLockWasNotReceivedBecauseAlreadHadShared()
        {
            _lockTable = new LockTable(TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(100));

            //Assert.Throws<LockAbortException>(() =>
            Assert.DoesNotThrow(() =>
            {
                var block = new Block(RandomFilename, 0);

                // The only way to receive a exclusive lock is only after contesting an shared lock
                _lockTable.SharedLock(block);
                _lockTable.ExclusiveLock(block);
            });
        }

        [Test]
        public void ThrowsExceptionIfSharedLockWasNotReceivedBecauseAlreadHadExclusive()
        {
            _lockTable = new LockTable(TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(100));

            Assert.Throws<LockAbortException>(() =>
            {
                var block = new Block(RandomFilename, 0);

                _lockTable.ExclusiveLock(block);
                _lockTable.SharedLock(block);
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
