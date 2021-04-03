using BB.IO.Primitives;
using BB.Transactions.Exceptions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace BB.Transactions.Concurrency
{
    public class LockTable
    {
        private readonly long _maxWaitingTime;
        private readonly int _tickWaitingTime;

        // TODO check performance with default dictionary
        private ConcurrentDictionary<Block, int> _locks;

        // TODO maybe do not use this?
        private readonly object _releaseLock = new object();

        public LockTable(
            TimeSpan? maxWaitingTime = null,
            TimeSpan? tickWaitingTime = null
            )
        {
            _locks = new ConcurrentDictionary<Block, int>();
            _maxWaitingTime = maxWaitingTime?.Ticks ?? TimeSpan.FromSeconds(5).Ticks;
            _tickWaitingTime = (int)(tickWaitingTime?.TotalMilliseconds ?? 200);
        }

        public void SharedLock(Block block)
        {
            long timestamp = DateTime.UtcNow.Ticks;

            while(HasExclusiveLock(block) && !WaitingForTooLong(timestamp))
            {
                Thread.Sleep(_tickWaitingTime);
            }

            if (HasExclusiveLock(block))
                throw new LockAbortException();

            var value = GetLockValue(block) + 1;
            _locks.AddOrUpdate(block, value, (block, val) => value);
        }

        public void ExclusiveLock(Block block)
        {
            long timestamp = DateTime.UtcNow.Ticks;

            while(HaveOtherSharedLocks(block) && !WaitingForTooLong(timestamp))
            {
                Thread.Sleep(_tickWaitingTime);
            }

            if (HaveOtherSharedLocks(block))
                throw new LockAbortException();

            _locks.AddOrUpdate(block, -1, (_, value) => -1);
        }

        public void Unlock(Block block)
        {
            try
            {
                Monitor.Enter(_releaseLock);

                var value = GetLockValue(block);
                if (value > 1)
                {
                    _locks.AddOrUpdate(block, value - 1, (_, val) => value - 1);
                }
                else
                {
                    _locks.Remove(block, out var _);
                }
            }
            finally
            {
                Monitor.Exit(_releaseLock);
            }
        }


        private bool HasExclusiveLock(Block block)
        {
            return GetLockValue(block) < 0;
        }

        private bool HaveOtherSharedLocks(Block block)
        {
            return GetLockValue(block) > 0;
        }

        private int GetLockValue(Block block)
        {
            var hasValue = _locks.TryGetValue(block, out var result);
            return hasValue ? result : 0;
        }


        private bool WaitingForTooLong(long startTime) 
        {
            return DateTime.UtcNow.Ticks - startTime > _maxWaitingTime;
        }
    }
}
