using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Transactions.Abstract;
using System;
using System.Collections.Generic;
using System.Text;

namespace BB.Transactions.Concurrency
{
    public class ConcurrencyManager : IConcurrencyManager
    {
        private readonly LockTable _lockTable;
        private readonly Dictionary<Block, ConcurrencyLockType> _locks;
        private readonly object _lock = new object();

        public ConcurrencyManager(LockTable lockTable)
        {
            _lockTable = lockTable;
            _locks = new Dictionary<Block, ConcurrencyLockType>();
        }

        public ConcurrencyManager()
        {
            _lockTable = new LockTable(null, null);
            _locks = new Dictionary<Block, ConcurrencyLockType>();
        }

        // TODO it should not cause concurrency issues,
        // but still need to check out
        public void SharedLock(Block block)
        {
            lock (_lock)
            {
                if (!_locks.TryGetValue(block, out var value))
                {
                    _lockTable.SharedLock(block);
                    _locks.Add(block, ConcurrencyLockType.Shared);
                }
            }
        }

        // TODO it should not cause concurrency issues,
        // but still need to check out
        public void ExclusiveLock(Block block)
        {
            lock (_lock)
            {
                if (!HasExclusiveLock(block))
                {
                    _lockTable.ExclusiveLock(block);
                    _locks[block] = ConcurrencyLockType.Exclusive;
                }
            }
        }

        // TODO it should not cause concurrency issues,
        // but still need to check out
        public void Release()
        {
            lock (_lock)
            {
                foreach (var block in _locks.Keys)
                {
                    _lockTable.Unlock(block);
                }

                _locks.Clear();
            }
        }

        private bool HasExclusiveLock(Block block)
        {
            var hasLock = _locks.TryGetValue(block, out var value);
            return !hasLock && value != ConcurrencyLockType.Exclusive;
        }

        internal enum ConcurrencyLockType : byte
        {
            Shared = 0,
            Exclusive = 1
        }
    }
}
