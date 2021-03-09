using BB.IO.Abstract;
using BB.IO.Primitives;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BB.IO.Tests
{
    public class FileManagerTests
    {
        private IFileManager _fileManager;

        [SetUp]
        public void SetUp()
        {
            if(!Directory.Exists("Logs"))
            {
                Directory.CreateDirectory("Logs");
            }

            _fileManager = new FileManager("Logs/test.tmp", 10);
        }

        [Test]
        public void DbFileExists()
        {
            Assert.IsTrue(File.Exists("Logs/test.tmp"));
        }

        [Test]
        public void CanAppendMemoryBlock()
        {
            var page = _fileManager.Append();

            Assert.IsNotNull(page);
            Assert.AreEqual(0, page.BlockId);
            Assert.AreEqual(10, page.PageSize);
            Assert.AreEqual(PageStatus.Commited, page.PageStatus);
            Assert.AreEqual(10, page.Data.Length);
        }

        [Test]
        public void CanWriteMemoryBlock()
        {
            var page = _fileManager.Append();
            page._data[0] = 1;
            _fileManager.Write(page);

            Assert.IsNotNull(page);
            Assert.AreEqual(0, page.BlockId);
            Assert.AreEqual(10, page.PageSize);
            Assert.AreEqual(PageStatus.Commited, page.PageStatus);
            Assert.AreEqual(10, page.Data.Length);
            Assert.AreEqual(1, page.Data[0]);
        }

        [Test]
        public void CanReadFirstMemoryBlock()
        {
            var page = _fileManager.Append();

            page._data[0] = 1;
            page._data[1] = 2;
            page._data[9] = 3;

            _fileManager.Write(page);

            _fileManager.Dispose();

            _fileManager = new FileManager("Logs/test.tmp", 10);
            var readResult = _fileManager.Read(0, out var readPage);

            Assert.IsTrue(readResult);
            Assert.IsNotNull(readPage);
            Assert.AreEqual(0, readPage.BlockId);
            Assert.AreEqual(10, readPage.PageSize);
            Assert.AreEqual(PageStatus.Commited, page.PageStatus);
            Assert.AreEqual(10, readPage.Data.Length);
            Assert.AreEqual(1, readPage.Data[0]);
            Assert.AreEqual(2, readPage.Data[1]);
            Assert.AreEqual(3, readPage.Data[9]);
        }

        [Test]
        public void CanReadNotFirstMemoryBlock()
        {
            var page = _fileManager.Append();
            var newPage = _fileManager.Append();

            newPage._data[0] = 1;
            newPage._data[1] = 2;
            newPage._data[9] = 3;

            _fileManager.Write(newPage);

            _fileManager.Dispose();

            _fileManager = new FileManager("Logs/test.tmp", 10);
            var readResult = _fileManager.Read(1, out var readPage);

            Assert.IsTrue(readResult);
            Assert.IsNotNull(readPage);
            Assert.AreEqual(1, readPage.BlockId);
            Assert.AreEqual(10, readPage.PageSize);
            Assert.AreEqual(PageStatus.Commited, page.PageStatus);
            Assert.AreEqual(10, readPage.Data.Length);
            Assert.AreEqual(1, readPage.Data[0]);
            Assert.AreEqual(2, readPage.Data[1]);
            Assert.AreEqual(3, readPage.Data[9]);
        }

        [Test]
        public void CannotReadInBadRange()
        {
            var result = _fileManager.Read(int.MaxValue, out var page);

            Assert.IsFalse(result);
            Assert.AreEqual(default(Page), page);
        }

        [Test]
        public void CannotWriteInBadRange()
        {
            var badPage = new Page(-1, 10);
            var result = _fileManager.Write(badPage);

            Assert.IsFalse(result);
        }

        [Test]
        public void CannotReadNegativeBlockId()
        {
            var result = _fileManager.Read(int.MaxValue, out var page);

            Assert.IsFalse(result);
            Assert.AreEqual(default(Page), page);
        }

        [Test]
        public void CannotWriteNegativeBlockId()
        {
            var badPage = new Page(-1, 10);
            var result = _fileManager.Write(badPage);

            Assert.IsFalse(result);
        }

        [Test]
        public void CannotWritePageWithPageSizeNotEqualToManagerBlockSize()
        {
            var badPage = new Page(0, 19);
            var result = _fileManager.Write(badPage);

            Assert.IsFalse(result);
        }

        [Test]
        public void EmptyManagerHasZeroLengthAndBlockSizeAndLastBlockId()
        {
            var length = _fileManager.Length;
            var blockSize = _fileManager.BlockSize;
            var lastBlockId = _fileManager.LastBlockId;

            Assert.AreEqual(0, length);
            Assert.AreEqual(10, blockSize);
            Assert.AreEqual(0, lastBlockId);
        }

        [Test]
        public void ManagerWithFilledPageHasCorrectLengthAndBlockSizeAndLastBlockId()
        {
            _ = _fileManager.Append();

            var length = _fileManager.Length;
            var blockSize = _fileManager.BlockSize;
            var lastBlockId = _fileManager.LastBlockId;

            Assert.AreEqual(1 * 10, length);
            Assert.AreEqual(10, blockSize);
            Assert.AreEqual(1, lastBlockId);
        }

        [TearDown]
        public void TearDown()
        {
            _fileManager?.Dispose();
            Directory.Delete("Logs", true);
        }
    }
}
