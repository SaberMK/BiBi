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
        public void DbFileExistsTests()
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
            Assert.IsNotNull(page.Data);
            Assert.AreEqual(10, page.Data.Length);
        }

        [Test]
        public void CanWriteBlock()
        {
            var page = _fileManager.Append();
            page.Data[0] = 1;
            _fileManager.Write(page);

            Assert.IsNotNull(page);
            Assert.AreEqual(0, page.BlockId);
            Assert.AreEqual(10, page.PageSize);
            Assert.AreEqual(PageStatus.Commited, page.PageStatus);
            Assert.IsNotNull(page.Data);
            Assert.AreEqual(10, page.Data.Length);
            Assert.AreEqual(1, page.Data[0]);
        }

        [TearDown]
        public void TearDown()
        {
            _fileManager.Dispose();
            Directory.Delete("Logs", true);
        }
    }
}
