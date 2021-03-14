using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Log;
using BB.Memory;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BB.IO.Tests
{
    public class DirectoryManagerTests
    {
        private IDirectoryManager _directoryManager;

        [SetUp]
        public void SetUp()
        {
            if (Directory.Exists("Logs"))
            {
                Directory.Delete("Logs", true);
            }

            _directoryManager = new DirectoryManager("Logs", 30);
        }

        [Test]
        public void CanCreateLogManager()
        {
            Assert.DoesNotThrow(() =>
            {
                var manager = _directoryManager.GetManager("dbLog.tmp");
                var logManager = new LogManager(manager);
            });
        }

        // DO NOT REMOVE THIS TEST!
        // This test would generously help on future, when I would work
        // on unmapped disk space
        [Test]
        public void CanCreateMultipleLogManagers()
        {
            Assert.DoesNotThrow(() =>
            {
                var manager1 = _directoryManager.GetManager("dbLog1.tmp");
                var logManager1 = new LogManager(manager1);

                var manager2 = _directoryManager.GetManager("dbLog2.tmp");
                var logManager2 = new LogManager(manager2);
            });
        }

        [Test]
        public void CanGetFileManager()
        {
            Assert.DoesNotThrow(() =>
            {
                var manager = _directoryManager.GetManager(GenerateRandomFilename());
            });
        }

        [Test]
        public void CanAppendPageToFile()
        {
            // WHY THE FUCK FILE NOT EXIST ON WINDOWS EXPLORER BUT EXISTS HERE???
            var page = _directoryManager.Append(GenerateRandomFilename());

            Assert.IsNotNull(page);
            Assert.AreEqual(30, page.Data.Length);
            Assert.AreEqual(0, page.BlockId);
        }

        [Test]
        public void CanWriteAndReadPage()
        {
            var filename = GenerateRandomFilename();

            var page = _directoryManager.Append(filename);
            page.SetInt(0, 123);

            var block = new Block(page.BlockId, filename);
            _directoryManager.Write(block, page);

            var canReadPage = _directoryManager.Read(block, out var readPage);

            Assert.IsTrue(canReadPage);
            Assert.AreEqual(page.PageSize, readPage.PageSize);
            Assert.AreEqual(page.Data[0], readPage.Data[0]);
            Assert.AreEqual(page.Data[1], readPage.Data[1]);
            Assert.AreEqual(page.Data[2], readPage.Data[2]);
            Assert.AreEqual(page.Data[3], readPage.Data[3]);
        }

        [TearDown]
        public void TearDown()
        {
            _directoryManager.Dispose();
            if (Directory.Exists("Logs"))
            {
                Directory.Delete("Logs", true);
            }
        }

        private string GenerateRandomFilename()
            => $"{Guid.NewGuid()}.tmp";
    }
}
