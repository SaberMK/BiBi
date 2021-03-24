using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Base;
using Moq;
using NUnit.Framework;
using System;
using System.IO;

namespace BB.Memory.Tests.Logger
{
    public class BasicLogRecordTests
    {
        private IFileManager _fileManager;
        private BasicLogRecord _logRecord;

        [SetUp]
        public void Setup()
        {
            var fileManagerMock = new Mock<IFileManager>();
            fileManagerMock.Setup(x => x.ResolvePage())
                .Returns(() => new Page(null, new Block("", 0), 100));

            _fileManager = fileManagerMock.Object;
        }

        [Test]
        public void CanCreateBasicLogRecord()
        {
            Assert.DoesNotThrow(() =>
            {
                var page = _fileManager.ResolvePage();
                _logRecord = new BasicLogRecord(page, 0);
            });
        }

        [Test]
        public void CanGetNextInt()
        {
            var page = _fileManager.ResolvePage();

            _logRecord = new BasicLogRecord(page, 0);

            page.SetInt(0, 123);

            var canGetRecord = _logRecord.NextInt(out var value);

            Assert.True(canGetRecord);
            Assert.AreEqual(123, value);
        }

        [Test]
        public void CanGetNextBool()
        {
            var page = _fileManager.ResolvePage();

            _logRecord = new BasicLogRecord(page, 0);

            page.SetBool(0, true);

            var canGetRecord = _logRecord.NextBool(out var value);

            Assert.True(canGetRecord);
            Assert.AreEqual(true, value);
        }

        [Test]
        public void CanGetNextByte()
        {
            var page = _fileManager.ResolvePage();

            _logRecord = new BasicLogRecord(page, 0);

            page.SetByte(0, 123);

            var canGetRecord = _logRecord.NextByte(out var value);

            Assert.True(canGetRecord);
            Assert.AreEqual(123, value);
        }

        [Test]
        public void CanGetNextBlob()
        {
            var page = _fileManager.ResolvePage();

            _logRecord = new BasicLogRecord(page, 0);

            page.SetBlob(0, new byte[] { 1, 2, 3 });

            var canGetRecord = _logRecord.NextBlob(out var value);

            Assert.True(canGetRecord);
            Assert.AreEqual(new byte[] { 1, 2, 3 }, value);
        }

        [Test]
        public void CanGetNextString()
        {
            var page = _fileManager.ResolvePage();

            _logRecord = new BasicLogRecord(page, 0);

            page.SetString(0, "123");

            var canGetRecord = _logRecord.NextString(out var value);

            Assert.True(canGetRecord);
            Assert.AreEqual("123", value);
        }

        [Test]
        public void CanGetNextDate()
        {
            var page = _fileManager.ResolvePage();

            _logRecord = new BasicLogRecord(page, 0);

            page.SetDate(0, new DateTime(2020, 1, 1));

            var canGetRecord = _logRecord.NextDate(out var value);

            Assert.True(canGetRecord);
            Assert.AreEqual(new DateTime(2020, 1, 1), value);
        }

        [Test]
        public void CannotGetWrongNextInt()
        {
            var page = _fileManager.ResolvePage();
            page.SetInt(0, 123);

            _logRecord = new BasicLogRecord(page, -1);


            var canGetRecord = _logRecord.NextInt(out var value);

            Assert.False(canGetRecord);
            Assert.AreEqual(default(int), value);
        }

        [Test]
        public void CannotGetWrongNextBool()
        {
            var page = _fileManager.ResolvePage();
            page.SetBool(0, true);

            _logRecord = new BasicLogRecord(page, -1);


            var canGetRecord = _logRecord.NextBool(out var value);

            Assert.False(canGetRecord);
            Assert.AreEqual(default(bool), value);
        }

        [Test]
        public void CannotGetWrongNextByte()
        {
            var page = _fileManager.ResolvePage();
            page.SetByte(0, 123);

            _logRecord = new BasicLogRecord(page, -1);

            var canGetRecord = _logRecord.NextByte(out var value);

            Assert.False(canGetRecord);
            Assert.AreEqual(default(byte), value);
        }

        [Test]
        public void CannotGetWrongNextBlob()
        {
            var page = _fileManager.ResolvePage();
            page.SetBlob(0, new byte[] { 1, 2, 3 });

            _logRecord = new BasicLogRecord(page, -1);

            var canGetRecord = _logRecord.NextBlob(out var value);

            Assert.False(canGetRecord);
            Assert.AreEqual(default(byte[]), value);
        }

        [Test]
        public void CannotGetWrongNextString()
        {
            var page = _fileManager.ResolvePage();
            page.SetString(0, "123");

            _logRecord = new BasicLogRecord(page, -1);

            var canGetRecord = _logRecord.NextString(out var value);

            Assert.False(canGetRecord);
            Assert.AreEqual(default(string[]), value);
        }


        [Test]
        public void CannotGetWrongNextDate()
        {
            var page = _fileManager.ResolvePage();
            page.SetDate(0, new DateTime(2020, 1, 1));

            _logRecord = new BasicLogRecord(page, -1);

            var canGetRecord = _logRecord.NextDate(out var value);

            Assert.False(canGetRecord);
            Assert.AreEqual(default(DateTime), value);
        }
    }
}
