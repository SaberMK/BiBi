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
        private Dictionary<Block, ConcurrencyLockType> _locks;

        public ConcurrencyManager(LockTable lockTable)
        {
            _lockTable = lockTable;
        }

        // TODO it should not cause concurrency issues,
        // but still need to check out
        public void SharedLock(Block block)
        {
            if(!_locks.TryGetValue(block, out var value))
            {
                _lockTable.SharedLock(block);
                _locks.Add(block, ConcurrencyLockType.Shared);
            }
        }

        // TODO it should not cause concurrency issues,
        // but still need to check out
        public void ExclusiveLock(Block block)
        {
            if (!HasExclusiveLock(block))
            {
                SharedLock(block);
                _lockTable.ExclusiveLock(block);
                _locks[block] = ConcurrencyLockType.Exclusive;
            }
        }

        // TODO it should not cause concurrency issues,
        // but still need to check out
        public void Release()
        {
            foreach(var block in _locks.Keys)
            {
                _lockTable.Unlock(block);
            }

            _locks.Clear();
        }

        private bool HasExclusiveLock(Block block)
        {
            var hasLock = _locks.TryGetValue(block, out var value);
            return !hasLock && value == ConcurrencyLockType.Exclusive;
        }

        internal enum ConcurrencyLockType : byte
        {
            Shared = 0,
            Exclusive = 1
        }
    }
}
