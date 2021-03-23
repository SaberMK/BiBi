using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Memory.Buffers.Formatters;
using BB.Memory.Logger;
using NUnit.Framework;

namespace BB.Memory.Tests.Buffers
{
    public class BufferTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;

        [SetUp]
        public void Setup()
        {
            _fileManager = new FileManager("temp", "DBs", 100);
            _logManager = new LogManager(_fileManager, $"log_{RandomFilename}");
        }

        [Test]
        public void CanCreateBuffer()
        {
            Assert.DoesNotThrow(() =>
            {
                var buffer = new Buffer(_logManager, _fileManager);
            });
        }

        [Test]
        public void CanPinBuffer()
        {
            var buffer = new Buffer(_logManager, _fileManager);

            var isPinned1 = buffer.IsPinned;

            buffer.Pin();

            var isPinned2 = buffer.IsPinned;

            Assert.IsFalse(isPinned1);
            Assert.IsTrue(isPinned2);
        }

        [Test]
        public void CanUnpinBuffer()
        {
            var buffer = new Buffer(_logManager, _fileManager);

            buffer.Pin();
            var isPinned1 = buffer.IsPinned;

            buffer.Unpin();
            var isPinned2 = buffer.IsPinned;

            Assert.IsTrue(isPinned1);
            Assert.IsFalse(isPinned2);
        }

        [Test]
        public void CanPinMultipleTimes()
        {
            var buffer = new Buffer(_logManager, _fileManager);
            buffer.Pin();
            var isPinned1 = buffer.IsPinned;
            buffer.Pin();
            var isPinned2 = buffer.IsPinned;
            buffer.Pin();
            var isPinned3 = buffer.IsPinned;
            buffer.Pin();
            var isPinned4 = buffer.IsPinned;

            Assert.IsTrue(isPinned1);
            Assert.IsTrue(isPinned2);
            Assert.IsTrue(isPinned3);
            Assert.IsTrue(isPinned4);
        }

        [Test]
        public void CanUnpinMultipleTimesWithoutThrowingException()
        {
            var buffer = new Buffer(_logManager, _fileManager);
            buffer.Pin();
            var isPinned1 = buffer.IsPinned;
            buffer.Unpin();
            var isPinned2 = buffer.IsPinned;
            buffer.Unpin();
            var isPinned3 = buffer.IsPinned;
            buffer.Unpin();
            var isPinned4 = buffer.IsPinned;

            Assert.IsTrue(isPinned1);
            Assert.IsFalse(isPinned2);
            Assert.IsFalse(isPinned3);
            Assert.IsFalse(isPinned4);
        }

        [Test]
        public void CanAssignToNewBlock()
        {
            var filename = RandomFilename;
            var buffer = new Buffer(_logManager, _fileManager);

            Assert.DoesNotThrow(() =>
            {
                buffer.AssignToNew(filename, new BasePageFormatter());
            });

            Assert.AreEqual(filename, buffer.Block.Filename);
            Assert.AreEqual(0, buffer.Block.Id);
        }

        [Test]
        public void CanAssignToAnExistingBlock()
        {
            var filename = RandomFilename;

            var buffer = new Buffer(_logManager, _fileManager);
            var block = new Block(filename, 0);
            Assert.DoesNotThrow(() =>
            {
                buffer.AssignToBlock(block);
            });

            Assert.AreEqual(filename, buffer.Block.Filename);
            Assert.AreEqual(0, buffer.Block.Id);
        }

        [Test]
        public void CanReadDataFromDIskWhenAssignesToExistingBlock()
        {
            var filename = RandomFilename;

            var buffer = new Buffer(_logManager, _fileManager);
            var block = new Block(filename, 0);

            var page = _fileManager.ResolvePage(block);
            var canSetInt = page.SetInt(0, 123);
            var canWrite = page.Write(block);

            Assert.DoesNotThrow(() =>
            {
                buffer.AssignToBlock(block);
            });

            var canRead = buffer.GetInt(0, out var value);

            Assert.IsTrue(canSetInt);
            Assert.IsTrue(canWrite);
            Assert.AreEqual(filename, buffer.Block.Filename);
            Assert.AreEqual(0, buffer.Block.Id);
            Assert.IsTrue(canRead);
            Assert.AreEqual(123, value);
        }

        [Test]
        public void CanWriteInt()
        {
            var source = 123;
            var filename = RandomFilename;
            var buffer = new Buffer(_logManager, _fileManager);

            buffer.AssignToNew(filename, new BasePageFormatter());

            buffer.SetInt(0, source, 1, 1);
            buffer.Flush();

            var page = _fileManager.ResolvePage();
            page.Read(buffer.Block);
            var canRead = page.GetInt(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(source, value);
        }

        [Test]
        public void CanReadInt()
        {
            var source = 123;
            var filename = RandomFilename;

            var buffer = new Buffer(_logManager, _fileManager);
            var block = new Block(filename, 0);

            var page = _fileManager.ResolvePage(block);
            page.SetInt(0, source);
            page.Write(block);

            buffer.AssignToBlock(block);

            var canRead = buffer.GetInt(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(source, value);
        }

        [Test]
        public void CanWriteByte()
        {
            byte source = 123;
            var filename = RandomFilename;
            var buffer = new Buffer(_logManager, _fileManager);

            buffer.AssignToNew(filename, new BasePageFormatter());

            buffer.SetByte(0, source, 1, 1);
            buffer.Flush();

            var page = _fileManager.ResolvePage();
            page.Read(buffer.Block);
            var canRead = page.GetByte(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(source, value);
        }


        [Test]
        public void CanReadByte()
        {
            byte source = 123;
            var filename = RandomFilename;

            var buffer = new Buffer(_logManager, _fileManager);
            var block = new Block(filename, 0);

            var page = _fileManager.ResolvePage(block);
            page.SetByte(0, source);
            page.Write(block);

            buffer.AssignToBlock(block);

            var canRead = buffer.GetByte(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(source, value);
        }

        [Test]
        public void CanWriteBool()
        {
            var source = true;
            var filename = RandomFilename;
            var buffer = new Buffer(_logManager, _fileManager);

            buffer.AssignToNew(filename, new BasePageFormatter());

            buffer.SetBool(0, source, 1, 1);
            buffer.Flush();

            var page = _fileManager.ResolvePage();
            page.Read(buffer.Block);
            var canRead = page.GetBool(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(source, value);
        }


        [Test]
        public void CanReadBool()
        {
            var source = true;
            var filename = RandomFilename;

            var buffer = new Buffer(_logManager, _fileManager);
            var block = new Block(filename, 0);

            var page = _fileManager.ResolvePage(block);
            page.SetBool(0, source);
            page.Write(block);

            buffer.AssignToBlock(block);

            var canRead = buffer.GetBool(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(source, value);
        }

        [Test]
        public void CanWriteBlob()
        {
            var source = new byte[] { 1, 2, 3 };
            var filename = RandomFilename;
            var buffer = new Buffer(_logManager, _fileManager);

            buffer.AssignToNew(filename, new BasePageFormatter());

            buffer.SetBlob(0, source, 1, 1);
            buffer.Flush();

            var page = _fileManager.ResolvePage();
            page.Read(buffer.Block);
            var canRead = page.GetBlob(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(source, value);
        }

        [Test]
        public void CanReadBlob()
        {
            var source = new byte[] { 1, 2, 3 };
            var filename = RandomFilename;

            var buffer = new Buffer(_logManager, _fileManager);
            var block = new Block(filename, 0);

            var page = _fileManager.ResolvePage(block);
            page.SetBlob(0, source);
            page.Write(block);

            buffer.AssignToBlock(block);

            var canRead = buffer.GetBlob(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(source, value);
        }

        [Test]
        public void CanWriteString()
        {
            var source = "123";
            var filename = RandomFilename;
            var buffer = new Buffer(_logManager, _fileManager);

            buffer.AssignToNew(filename, new BasePageFormatter());

            buffer.SetString(0, source, 1, 1);
            buffer.Flush();

            var page = _fileManager.ResolvePage();
            page.Read(buffer.Block);
            var canRead = page.GetString(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(source, value);
        }

        [Test]
        public void CanReadString()
        {
            var source = "123";
            var filename = RandomFilename;

            var buffer = new Buffer(_logManager, _fileManager);
            var block = new Block(filename, 0);

            var page = _fileManager.ResolvePage(block);
            page.SetString(0, source);
            page.Write(block);

            buffer.AssignToBlock(block);

            var canRead = buffer.GetString(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(source, value);
        }

        [Test]
        public void CanWriteDate()
        {
            var source = new System.DateTime(2020, 1, 1);
            var filename = RandomFilename;
            var buffer = new Buffer(_logManager, _fileManager);

            buffer.AssignToNew(filename, new BasePageFormatter());

            buffer.SetDate(0, source, 1, 1);
            buffer.Flush();

            var page = _fileManager.ResolvePage();
            page.Read(buffer.Block);
            var canRead = page.GetDate(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(source, value);
        }

        [Test]
        public void CanReadDate()
        {
            var source = new System.DateTime(2020, 1, 1);
            var filename = RandomFilename;

            var buffer = new Buffer(_logManager, _fileManager);
            var block = new Block(filename, 0);

            var page = _fileManager.ResolvePage(block);
            page.SetDate(0, source);
            page.Write(block);

            buffer.AssignToBlock(block);

            var canRead = buffer.GetDate(0, out var value);

            Assert.IsTrue(canRead);
            Assert.AreEqual(source, value);
        }

        [Test]
        public void IsModifiedByCurrentTransaction()
        {
            var source = 123;
            var filename = RandomFilename;
            var buffer = new Buffer(_logManager, _fileManager);

            buffer.AssignToNew(filename, new BasePageFormatter());

            buffer.SetInt(0, source, 1, 1);
            var modifiedBy = buffer.IsModifiedBy(1);

            Assert.IsTrue(modifiedBy);
        }

        private string RandomFilename => $"{System.Guid.NewGuid()}.bin";
    }
}
