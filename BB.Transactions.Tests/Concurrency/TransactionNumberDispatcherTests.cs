using BB.Transactions.Concurrency;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Text;

namespace BB.Transactions.Tests.Concurrency
{
    public class TransactionNumberDispatcherTests
    {
        private TransactionNumberDispatcher _dispatcher;

        [Test]
        public void CanCreateTransactionNumberDispatcher()
        {
            Assert.DoesNotThrow(() =>
            {
                _dispatcher = new TransactionNumberDispatcher(0);
            });
        }


        [Test]
        public void CanCreateDefaultTransactionNumberDispatcher()
        {
            Assert.DoesNotThrow(() =>
            {
                _dispatcher = new TransactionNumberDispatcher();
            });
        }

        [Test]
        public void CanGetTransactionNumber()
        {
            _dispatcher = new TransactionNumberDispatcher(10);

            var number = _dispatcher.GetNextTransactionNumber();
            var nextNumber = _dispatcher.GetNextTransactionNumber();

            Assert.AreEqual(11, number);
            Assert.AreEqual(12, nextNumber);
        }
    }
}
