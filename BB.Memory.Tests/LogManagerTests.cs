using BB.IO;
using BB.Memory.Abstract;
using BB.Memory.Log;
using NUnit.Framework;
using System.IO;

namespace BB.Memory.Tests
{
    public class LogManagerTests
    {
        private LogManager _logManager;

        [SetUp]
        public void Setup()
        {
            if (!Directory.Exists("Logs"))
            {
                Directory.CreateDirectory("Logs");
            }

            var fileManager = new FileManager("Logs/test.tmp", 20);
            _logManager = new LogManager(fileManager);
        }

        [Test]
        public void CanFitARecordToALog()
        {
            var data = new byte[] { 1, 2, 3 };
            var result = _logManager.Append(data, out var lsn);

            var boundary = _logManager.Boundary;
            var latestLsn = _logManager.LatestLSN;

            Assert.IsTrue(result);
            Assert.AreEqual(1, lsn);
            Assert.AreEqual(7, boundary);
            Assert.AreEqual(1, latestLsn);
        }

        // maybe would fix it in future...
        [Test]
        public void CannotPushLogRecordGreaterThanLogPage()
        {
            var data = new byte[20];
            var result = _logManager.Append(data, out var lsn);

            Assert.IsFalse(result);
        }

        [Test]
        public void CanFitACoupleOfRecordsToALog()
        {
            var data1 = new byte[] { 1, 1 };
            var data2 = new byte[] { 2, 2, };

            var res1 = _logManager.Append(data1, out var lsn1);
            var res2 = _logManager.Append(data2, out var lsn2);

            Assert.IsTrue(res1);
            Assert.IsTrue(res2);
            Assert.AreEqual(1, lsn1);
            Assert.AreEqual(2, lsn2);
            Assert.AreEqual(12, _logManager.Boundary);
            Assert.AreEqual(2, _logManager.LatestLSN);
        }

        [Test]
        public void CanFitAndSaveACoupleOfRecordsToALog()
        {
            var data1 = new byte[] { 1, 1 };
            var data2 = new byte[] { 2, 2, };

            var res1 = _logManager.Append(data1, out var lsn1);
            var res2 = _logManager.Append(data2, out var lsn2);

            _logManager.Flush(lsn2);

            Assert.IsTrue(res1);
            Assert.IsTrue(res2);
            Assert.AreEqual(1, lsn1);
            Assert.AreEqual(2, lsn2);
            Assert.AreEqual(12, _logManager.Boundary);
            Assert.AreEqual(2, _logManager.LatestLSN);
            Assert.AreEqual(2, _logManager.LatestSavedLSN);
        }

        [Test]
        public void CanFitAndSaveRecordsOnMultiplePages()
        {
            var data1 = new byte[12];
            var data2 = new byte[12];

            var res1 = _logManager.Append(data1, out var lsn1);
            var res2 = _logManager.Append(data2, out var lsn2);

            Assert.IsTrue(res1);
            Assert.IsTrue(res2);
            Assert.AreEqual(1, lsn1);
            Assert.AreEqual(2, lsn2);
            Assert.AreEqual(16, _logManager.Boundary);
            Assert.AreEqual(2, _logManager.LatestLSN);
            Assert.AreEqual(1, _logManager.LatestSavedLSN);
        }

        [TearDown]
        public void TearDown()
        {
            _logManager?.Dispose();
            Directory.Delete("Logs", true);
        }
    }
}