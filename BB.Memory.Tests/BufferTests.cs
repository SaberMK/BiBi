using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Log;
using NUnit.Framework;
using BB.Memory.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BB.Memory.Tests
{
    public class BufferTests
    {
        private IDirectoryManager _directoryManager;
        private ILogManager _logManager;
        private IFileManager _fileManager;

        [SetUp]
        public void SetUp()
        {
            if (Directory.Exists("Logs"))
            {
                Directory.Delete("Logs", true);
            }

            _directoryManager = new DirectoryManager("Logs", 30);
            var logFileManager = _directoryManager.GetManager(GetRandomFilename());
            _logManager = new LogManager(logFileManager);
            _fileManager = _directoryManager.GetManager(GetRandomFilename());
        }

        [Test]
        public void CanCreateBuffer()
        {
            Assert.DoesNotThrow(() =>
            {
                var buffer = new Buffer(_logManager);
            });
        }

        [Test]
        public void CanPinBuffer()
        {
            var buffer = new Buffer(_logManager);
            buffer.AssignToBlock(0, _fileManager);
            buffer.Pin();

            Assert.IsNotNull(buffer);
            Assert.True(buffer.IsPinned);
        }

        [Test]
        public void CanPinAndUnpinBuffer()
        {
            var buffer = new Buffer(_logManager);
            buffer.AssignToBlock(0, _fileManager);
            buffer.Pin();

            Assert.IsNotNull(buffer);
            Assert.True(buffer.IsPinned);

            buffer.Unpin();
            Assert.AreEqual(false, buffer.IsPinned);
        }

        [Test]
        public void CanPinMultipleTimes()
        {
            var buffer = new Buffer(_logManager);
            buffer.AssignToBlock(0, _fileManager);
            buffer.Pin();

            Assert.IsNotNull(buffer);
            Assert.True(buffer.IsPinned);

            buffer.Pin();
            Assert.True(buffer.IsPinned);

            buffer.Unpin();
            Assert.True(buffer.IsPinned);

            buffer.Unpin();
            Assert.AreEqual(false, buffer.IsPinned);
        }

        [Test]
        public void CanWriteToBufferPage()
        {
            var buffer = new Buffer(_logManager);
            buffer.AssignToBlock(0, _fileManager);

            buffer.Pin();
            var page = buffer.Page;
            var canWrite = page.SetInt(5, 1024);
            buffer.Unpin();

            var canRead = page.GetInt(5, out var result);

            Assert.AreEqual(false, buffer.IsPinned);
            Assert.True(canWrite);
            Assert.True(canRead);
            Assert.AreEqual(1024, result);
        }

        [Test]
        public void CanFlushBuffer()
        {
            var buffer = new Buffer(_logManager);
            buffer.AssignToBlock(0, _fileManager);

            buffer.Pin();
            var page = buffer.Page;
            var canWrite = page.SetInt(5, 1024);
            buffer.SetModified(1, 1);
            buffer.Unpin();

            buffer.Flush();

            var canReadPage = _fileManager.Read(0, out var newPage);
            var canReadFromPage = newPage.GetInt(5, out var result);

            Assert.AreEqual(false, buffer.IsPinned);
            Assert.True(canWrite);
            Assert.True(canReadPage);
            Assert.True(canReadFromPage);
            Assert.AreEqual(1024, result);
        }

        [Test]
        public void WouldNotWriteUnstagedChangesToDisk()
        {
            var buffer = new Buffer(_logManager);
            buffer.AssignToBlock(0, _fileManager);

            buffer.Pin();
            var page = buffer.Page;
            var canWrite = page.SetInt(5, 1024);
            buffer.Unpin();

            buffer.Flush();

            var canReadPage = _fileManager.Read(0, out var newPage);
            var canReadFromPage = newPage.GetInt(5, out var result);

            Assert.AreEqual(false, buffer.IsPinned);
            Assert.True(canWrite);
            Assert.True(canReadPage);
            Assert.True(canReadFromPage);
            Assert.AreEqual(0, result);
        }

        [TearDown]
        public void TearDown()
        {
            _logManager.Dispose();
            _fileManager.Dispose();
            _directoryManager.Dispose();
        }

        private string GetRandomFilename() => $"{System.Guid.NewGuid()}.tmp";
    }
}
