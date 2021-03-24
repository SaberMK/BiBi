using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Logger;
using NUnit.Framework;
using System;
using System.IO;

namespace BB.Memory.Tests.Logger
{
    public class LogManagerTests
    {
        private IFileManager _fileManager;
        private ILogManager _logManager;

        [SetUp]
        public void Setup()
        {
            _fileManager = new FileManager("temp", "DBs", 100);
        }

        [Test]
        public void CanCreateLogManager()
        {
            Assert.DoesNotThrow(() =>
            {
                _logManager = new LogManager(_fileManager, RandomFilename);
            });
        }

        [Test]
        public void CanAppendIntToLog()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);
            var canAppend = _logManager.Append(new object[] { 123 }, out var lsn);
            _logManager.Flush(lsn);

            var page = _fileManager.ResolvePage();
            var canReadPage = page.Read(new Block(filename, lsn));

            var canGetTotal = page.GetInt(0, out var totalBytes);
            var canReadValue = page.GetInt(4, out var value);

            Assert.True(canAppend);
            Assert.True(canReadPage);
            Assert.True(canGetTotal);
            Assert.True(canReadValue);
            Assert.AreEqual(sizeof(int) + sizeof(int), totalBytes);
            Assert.AreEqual(123, value);
        }

        [Test]
        public void CanAppendByteToLog()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);
            var canAppend = _logManager.Append(new object[] { (byte)123 }, out var lsn);
            _logManager.Flush(lsn);

            var page = _fileManager.ResolvePage();
            var canReadPage = page.Read(new Block(filename, lsn));

            var canGetTotal = page.GetInt(0, out var totalBytes);
            var canReadValue = page.GetByte(4, out var value);

            Assert.True(canAppend);
            Assert.True(canReadPage);
            Assert.True(canGetTotal);
            Assert.True(canReadValue);
            Assert.AreEqual(sizeof(int) + sizeof(byte), totalBytes);
            Assert.AreEqual(123, value);
        }

        [Test]
        public void CanAppendBoolToLog()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);
            var canAppend = _logManager.Append(new object[] { true }, out var lsn);
            _logManager.Flush(lsn);

            var page = _fileManager.ResolvePage();
            var canReadPage = page.Read(new Block(filename, lsn));

            var canGetTotal = page.GetInt(0, out var totalBytes);
            var canReadValue = page.GetBool(4, out var value);

            Assert.True(canAppend);
            Assert.True(canReadPage);
            Assert.True(canGetTotal);
            Assert.True(canReadValue);
            Assert.AreEqual(sizeof(int) + sizeof(bool), totalBytes);
            Assert.AreEqual(true, value);
        }

        [Test]
        public void CanAppendBlobToLog()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);
            var arr = new byte[] { 1, 2, 3 };
            var canAppend = _logManager.Append(new object[] { arr }, out var lsn);
            _logManager.Flush(lsn);

            var page = _fileManager.ResolvePage();
            var canReadPage = page.Read(new Block(filename, lsn));

            var canGetTotal = page.GetInt(0, out var totalBytes);
            var canReadValue = page.GetBlob(4, out var value);

            Assert.True(canAppend);
            Assert.True(canReadPage);
            Assert.True(canGetTotal);
            Assert.True(canReadValue);
            Assert.AreEqual(sizeof(int) + sizeof(int) + arr.Length, totalBytes);
            Assert.AreEqual(arr, value);
        }

        [Test]
        public void CanAppendStringToLog()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);
            var str = "123123";
            var canAppend = _logManager.Append(new object[] { str }, out var lsn);
            _logManager.Flush(lsn);

            var page = _fileManager.ResolvePage();
            var canReadPage = page.Read(new Block(filename, lsn));

            var canGetTotal = page.GetInt(0, out var totalBytes);
            var canReadValue = page.GetString(4, out var value);

            Assert.True(canAppend);
            Assert.True(canReadPage);
            Assert.True(canGetTotal);
            Assert.True(canReadValue);
            Assert.AreEqual(sizeof(int) + sizeof(int) + str.Length, totalBytes);
            Assert.AreEqual(str, value);
        }

        [Test]
        public void CanAppendDateToLog()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);
            var date = new DateTime(2020, 1, 1);
            var canAppend = _logManager.Append(new object[] { date }, out var lsn);
            _logManager.Flush(lsn);

            var page = _fileManager.ResolvePage();
            var canReadPage = page.Read(new Block(filename, lsn));

            var canGetTotal = page.GetInt(0, out var totalBytes);
            var canReadValue = page.GetDate(4, out var value);

            Assert.True(canAppend);
            Assert.True(canReadPage);
            Assert.True(canGetTotal);
            Assert.True(canReadValue);
            Assert.AreEqual(sizeof(int) + sizeof(long), totalBytes);
            Assert.AreEqual(date, value);
        }

        [Test]
        public void CannotInsertRecordLongerThanALogRecord()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);

            var objects = new object[101];
            for (int i = 0; i < 101; ++i)
                objects[i] = true;

            var canAppend = _logManager.Append(objects, out var lsn);

            Assert.IsFalse(canAppend);
            Assert.AreEqual(default(int), lsn);
        }

        [Test]
        public void CanApplyLogToExistingFile()
        {
            var filename = RandomFilename;
            _fileManager.Append(filename, out _);
            _fileManager.Append(filename, out var block);

            Assert.DoesNotThrow(() =>
            {
                _logManager = new LogManager(_fileManager, filename);
            });
        }

        [Test]
        public void WouldNotWriteUnexpectedEntity()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);

            var canAppend = _logManager.Append(new object[] { new object() }, out var lsn);

            Assert.IsFalse(canAppend);
            Assert.AreEqual(default(int), lsn);
        }

        [Test]
        public void CanAddMultipleRecordsInOneBlock()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);

            var record = new object[]
            {
                10,
                12,
                "123123",
                true,
                (byte)(123),
                new byte[]{ 1,2,3,4,5,6,7,8,9, 0}
            };

            var canAppend1 = _logManager.Append(record, out var lsn1);
            var canAppend2 = _logManager.Append(record, out var lsn2);

            Assert.IsTrue(canAppend1);
            Assert.AreEqual(0, lsn1);
            Assert.IsTrue(canAppend2);
            Assert.AreEqual(0, lsn2);
        }

        [Test]
        public void CanAddMultipleRecordsInDifferentBlocks()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);

            var record = new object[]
            {
                10,
                12,
                "123123",
                true,
                (byte)(123),
                new byte[]{ 1,2,3,4,5,6,7,8,9, 0}
            };

            var canAppend1 = _logManager.Append(record, out var lsn1);
            var canAppend2 = _logManager.Append(record, out var lsn2);
            var canAppend3 = _logManager.Append(record, out var lsn3);

            Assert.IsTrue(canAppend1);
            Assert.AreEqual(0, lsn1);
            Assert.IsTrue(canAppend2);
            Assert.AreEqual(0, lsn2);
            Assert.IsTrue(canAppend3);
            Assert.AreEqual(1, lsn3);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
