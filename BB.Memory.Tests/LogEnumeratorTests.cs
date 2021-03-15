using BB.IO;
using BB.Memory.Abstract;
using BB.Memory.Log;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BB.Memory.Tests
{
    public class LogEnumeratorTests
    {
        private ILogManager _logManager;

        [SetUp]
        public void SetUp()
        {
            if (Directory.Exists("Logs"))
            {
                Directory.Delete("Logs", true);
            }

            Directory.CreateDirectory("Logs");

            var fileManager = new FileManager($"Logs/{Guid.NewGuid()}.tmp", 20);
            _logManager = new LogManager(fileManager);
        }

        [Test]
        public void CanGetEnumerator()
        {
            Assert.DoesNotThrow(() =>
            {
                var iterator = _logManager.Enumerator();
            });
        }

        [Test]
        public void CannotReadEmptyLog()
        {
            using (var enumerator = _logManager.Enumerator())
            {
                var canRead = enumerator.MoveNext();

                Assert.False(canRead);
            }
        }

        [Test]
        public void CanReadOneLogEntry()
        {
            var logEntry = new byte[] { 1, 2, 3, 4 };
            var isSuccessful = _logManager.Append(logEntry, out var lsn);

            Assert.IsTrue(isSuccessful);

            _logManager.Flush(lsn);

            using (var enumerator = _logManager.Enumerator())
            {
                var current = enumerator.Current;
                var canMoveNext = enumerator.MoveNext();



                Assert.AreEqual(current, logEntry);
                Assert.False(canMoveNext);
            }
        }

        [Test]
        public void CannotReadLogWhenPageNotFlushedEntry()
        {
            var logEntry = new byte[] { 1, 2, 3, 4 };
            var isSuccessful = _logManager.Append(logEntry, out var lsn);

            Assert.IsTrue(isSuccessful);

            using (var enumerator = _logManager.Enumerator())
            {
                var current = enumerator.Current;
                var canMoveNext = enumerator.MoveNext();

                Assert.AreEqual(null, current);
                Assert.False(canMoveNext);
            }
        }

        [Test]
        public void CanReadSeveralLogEntries()
        {
            var logEntry1 = new byte[] { 1, 2 };
            var logEntry2 = new byte[] { 3, 4 };

            var isSuccessful1 = _logManager.Append(logEntry1, out var lsn1);
            var isSuccessful2 = _logManager.Append(logEntry2, out var lsn2);

            Assert.IsTrue(isSuccessful1);
            Assert.IsTrue(isSuccessful2);

            _logManager.Flush(lsn2);

            using(var enumerator = _logManager.Enumerator())
            {
                var current2 = enumerator.Current;
                var canMoveNext2 = enumerator.MoveNext();

                var current1 = enumerator.Current;
                var canMoveNext1 = enumerator.MoveNext();

                Assert.AreEqual(current2, logEntry2);
                Assert.IsTrue(canMoveNext2);
                Assert.AreEqual(current1, logEntry1);
                Assert.IsFalse(canMoveNext1);
            }
        }

        [Test]
        public void CanReadSeveralLogEntriesEveryOnDifferentPageFromDifferentPages()
        {
            var logEntry1 = new byte[] { 1, 2, 3, 4, 5 };
            var logEntry2 = new byte[] { 6, 7, 8, 9, 10 };
            var logEntry3 = new byte[] { 11, 12, 13, 14, 15 };

            var isSuccessful1 = _logManager.Append(logEntry1, out var lsn1);
            var isSuccessful2 = _logManager.Append(logEntry2, out var lsn2);
            var isSuccessful3 = _logManager.Append(logEntry3, out var lsn3);

            Assert.IsTrue(isSuccessful1);
            Assert.IsTrue(isSuccessful2);
            Assert.IsTrue(isSuccessful3);

            _logManager.Flush(lsn3);

            using(var enumerator = _logManager.Enumerator())
            {
                var current3 = enumerator.Current;
                var canMoveNext3 = enumerator.MoveNext();

                var current2 = enumerator.Current;
                var canMoveNext2 = enumerator.MoveNext();

                var current1 = enumerator.Current;
                var canMoveNext1 = enumerator.MoveNext();

                Assert.AreEqual(current3, logEntry3);
                Assert.IsTrue(canMoveNext3);
                Assert.AreEqual(current2, logEntry2);
                Assert.IsTrue(canMoveNext2);
                Assert.AreEqual(current1, logEntry1);
                Assert.IsFalse(canMoveNext1);
            }
        }

        [Test]
        public void CanReadSeveralLogEntriesFromDifferentPages()
        {
            var logEntry1 = new byte[] { 1, 2, 3};
            var logEntry2 = new byte[] { 4,5,6};
            var logEntry3 = new byte[] { 7,8,9 };
            var logEntry4 = new byte[] { 10,11,12};

            var isSuccessful1 = _logManager.Append(logEntry1, out var lsn1);
            var isSuccessful2 = _logManager.Append(logEntry2, out var lsn2);
            var isSuccessful3 = _logManager.Append(logEntry3, out var lsn3);
            var isSuccessful4 = _logManager.Append(logEntry4, out var lsn4);

            Assert.IsTrue(isSuccessful1);
            Assert.IsTrue(isSuccessful2);
            Assert.IsTrue(isSuccessful3);
            Assert.IsTrue(isSuccessful4);

            _logManager.Flush(lsn4);

            using (var enumerator = _logManager.Enumerator())
            {
                var current4 = enumerator.Current;
                var canMoveNext4 = enumerator.MoveNext();

                var current3 = enumerator.Current;
                var canMoveNext3 = enumerator.MoveNext();

                var current2 = enumerator.Current;
                var canMoveNext2 = enumerator.MoveNext();

                var current1 = enumerator.Current;
                var canMoveNext1 = enumerator.MoveNext();

                Assert.AreEqual(current4, logEntry4);
                Assert.IsTrue(canMoveNext4);
                Assert.AreEqual(current3, logEntry3);
                Assert.IsTrue(canMoveNext3);
                Assert.AreEqual(current2, logEntry2);
                Assert.IsTrue(canMoveNext2);
                Assert.AreEqual(current1, logEntry1);
                Assert.IsFalse(canMoveNext1);
            }
        }

        [TearDown]
        public void TearDown()
        {
            _logManager?.Dispose();
        }
    }
}
