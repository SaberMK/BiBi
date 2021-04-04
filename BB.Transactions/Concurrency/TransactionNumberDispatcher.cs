using BB.Transactions.Abstract;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace BB.Transactions.Concurrency
{
    public class TransactionNumberDispatcher : ITransactionNumberDispatcher
    {
        private int _nextTransactionNumber;

        public TransactionNumberDispatcher(int nextTransactionNumber = 0)
        {
            _nextTransactionNumber = nextTransactionNumber;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public int GetNextTransactionNumber()
        {
            _nextTransactionNumber++;
            return _nextTransactionNumber;
        }
    }
}
