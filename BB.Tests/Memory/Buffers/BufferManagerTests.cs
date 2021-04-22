using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Formatters;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Exceptions;
using BB.Memory.Logger;
using NUnit.Framework;
using System;

namespace BB.Tests.Memory.Buffers
{
    public class BufferManagerTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private IPageFormatter _pageFormatter;
        private IBufferPoolStrategy _poolStrategy;
        private IBufferManager _bufferManager;

        [SetUp]
        public void Setup()
        {
            _fileManager = new FileManager("temp", "DBs", 100);
            _logManager = new LogManager(_fileManager, RandomFilename);
            _pageFormatter = new BasePageFormatter();

            // TODO MOCK POOL STRATEGY!!!
            // Or not? It seems default, mock would be like native...
            // Anyway, think about it a bit
        }

        [Test]
        public void CanCreateBufferManager()
        {
            Assert.DoesNotThrow(() =>
            {
                _poolStrategy = new LRUBufferPoolStrategy(_logManager, _fileManager, 3);
                _bufferManager = new BufferManager(_fileManager, _logManager, _poolStrategy, null, null);
            });
        }

        [Test]
        public void CanPinBuffer()
        {
            _poolStrategy = new LRUBufferPoolStrategy(_logManager, _fileManager, 3);
            _bufferManager = new BufferManager(_fileManager, _logManager, _poolStrategy, null, null);

            var filename = RandomFilename;

            var block = new Block(filename, 0);
            var buffer = _bufferManager.Pin(block);

            Assert.IsTrue(buffer.IsPinned);
            Assert.AreEqual(filename, buffer.Block.Filename);
            Assert.AreEqual(0, buffer.Block.Id);
            Assert.AreEqual(2, _bufferManager.Available);
        }

        [Test]
        public void CanPinSameBufferMultipleTimes()
        {
            _poolStrategy = new LRUBufferPoolStrategy(_logManager, _fileManager, 3);
            _bufferManager = new BufferManager(_fileManager, _logManager, _poolStrategy, null, null);

            var filename = RandomFilename;

            var block = new Block(filename, 0);
            var buffer1 = _bufferManager.Pin(block);
            var buffer2 = _bufferManager.Pin(block);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.IsTrue(buffer2.IsPinned);
            Assert.AreEqual(filename, buffer1.Block.Filename);
            Assert.AreEqual(0, buffer1.Block.Id);
            Assert.AreEqual(filename, buffer2.Block.Filename);
            Assert.AreEqual(0, buffer2.Block.Id);
            Assert.AreEqual(2, _bufferManager.Available);
        }

        [Test]
        public void CanPinAllBuffers()
        {
            _poolStrategy = new LRUBufferPoolStrategy(_logManager, _fileManager, 3);
            _bufferManager = new BufferManager(_fileManager, _logManager, _poolStrategy, null, null);

            var filename = RandomFilename;

            var block1 = new Block(filename, 0);
            var block2 = new Block(filename, 1);
            var block3 = new Block(filename, 2);
            var buffer1 = _bufferManager.Pin(block1);
            var buffer2 = _bufferManager.Pin(block2);
            var buffer3 = _bufferManager.Pin(block3);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.IsTrue(buffer2.IsPinned);
            Assert.IsTrue(buffer3.IsPinned);
            Assert.AreEqual(filename, buffer1.Block.Filename);
            Assert.AreEqual(0, buffer1.Block.Id);
            Assert.AreEqual(filename, buffer2.Block.Filename);
            Assert.AreEqual(1, buffer2.Block.Id);
            Assert.AreEqual(filename, buffer3.Block.Filename);
            Assert.AreEqual(2, buffer3.Block.Id);
            Assert.AreEqual(0, _bufferManager.Available);
        }

        [Test]
        public void CannotPinMoreBuffersThatIsOnPool()
        {
            _poolStrategy = new LRUBufferPoolStrategy(_logManager, _fileManager, 3);
            _bufferManager = new BufferManager(_fileManager, _logManager, _poolStrategy, TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(100));

            var filename = RandomFilename;

            var block1 = new Block(filename, 0);
            var block2 = new Block(filename, 1);
            var block3 = new Block(filename, 2);
            var block4 = new Block(filename, 3);
            var buffer1 = _bufferManager.Pin(block1);
            var buffer2 = _bufferManager.Pin(block2);
            var buffer3 = _bufferManager.Pin(block3);


            Assert.IsTrue(buffer1.IsPinned);
            Assert.IsTrue(buffer2.IsPinned);
            Assert.IsTrue(buffer3.IsPinned);
            Assert.AreEqual(filename, buffer1.Block.Filename);
            Assert.AreEqual(0, buffer1.Block.Id);
            Assert.AreEqual(filename, buffer2.Block.Filename);
            Assert.AreEqual(1, buffer2.Block.Id);
            Assert.AreEqual(filename, buffer3.Block.Filename);
            Assert.AreEqual(2, buffer3.Block.Id);
            Assert.Throws<BufferBusyException>(() =>
            {
                var buffer4 = _bufferManager.Pin(block4);
            });

            Assert.AreEqual(0, _bufferManager.Available);
        }

        [Test]
        public void CanPinNewBuffer()
        {
            _poolStrategy = new LRUBufferPoolStrategy(_logManager, _fileManager, 3);
            _bufferManager = new BufferManager(_fileManager, _logManager, _poolStrategy, null, null);

            var filename = RandomFilename;
            var pageFormatter = new BasePageFormatter();
            var buffer1 = _bufferManager.PinNew(filename, pageFormatter);
            var buffer2 = _bufferManager.PinNew(filename, pageFormatter);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.IsTrue(buffer2.IsPinned);
            Assert.AreEqual(filename, buffer1.Block.Filename);
            Assert.AreEqual(0, buffer1.Block.Id);
            Assert.AreEqual(filename, buffer2.Block.Filename);
            Assert.AreEqual(1, buffer2.Block.Id);
            Assert.AreEqual(1, _bufferManager.Available);
        }

        [Test]
        public void CannotPinMoreNewBlocksThatPoolHave()
        {
            _poolStrategy = new LRUBufferPoolStrategy(_logManager, _fileManager, 3);
            _bufferManager = new BufferManager(_fileManager, _logManager, _poolStrategy, TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(100));

            var filename = RandomFilename;
            var pageFormatter = new BasePageFormatter();

            var buffer1 = _bufferManager.PinNew(filename, pageFormatter);
            var buffer2 = _bufferManager.PinNew(filename, pageFormatter);
            var buffer3 = _bufferManager.PinNew(filename, pageFormatter);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.IsTrue(buffer2.IsPinned);
            Assert.IsTrue(buffer3.IsPinned);
            Assert.AreEqual(filename, buffer1.Block.Filename);
            Assert.AreEqual(0, buffer1.Block.Id);
            Assert.AreEqual(filename, buffer2.Block.Filename);
            Assert.AreEqual(1, buffer2.Block.Id);
            Assert.AreEqual(filename, buffer3.Block.Filename);
            Assert.AreEqual(2, buffer3.Block.Id);
            Assert.Throws<BufferBusyException>(() =>
            {
                var buffer4 = _bufferManager.PinNew(filename, pageFormatter);
            });

            Assert.AreEqual(0, _bufferManager.Available);
        }

        [Test]
        public void CanUnpinBuffer()
        {
            _poolStrategy = new LRUBufferPoolStrategy(_logManager, _fileManager, 3);
            _bufferManager = new BufferManager(_fileManager, _logManager, _poolStrategy, null, null);

            var filename = RandomFilename;
            var pageFormatter = new BasePageFormatter();
            var buffer1 = _bufferManager.PinNew(filename, pageFormatter);
            var buffer2 = _bufferManager.PinNew(filename, pageFormatter);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.IsTrue(buffer2.IsPinned);
            Assert.AreEqual(filename, buffer1.Block.Filename);
            Assert.AreEqual(0, buffer1.Block.Id);
            Assert.AreEqual(filename, buffer2.Block.Filename);
            Assert.AreEqual(1, buffer2.Block.Id);
            Assert.AreEqual(1, _bufferManager.Available);

            _bufferManager.Unpin(buffer1);
            Assert.IsFalse(buffer1.IsPinned);
            Assert.AreEqual(2, _bufferManager.Available);

            _bufferManager.Unpin(buffer2);
            Assert.IsFalse(buffer2.IsPinned);
            Assert.AreEqual(3, _bufferManager.Available);
        }

        [Test]
        public void CanPinBufferMultipleTimesAndUnpinMultipleTimes()
        {
            _poolStrategy = new LRUBufferPoolStrategy(_logManager, _fileManager, 3);
            _bufferManager = new BufferManager(_fileManager, _logManager, _poolStrategy, null, null);

            var filename = RandomFilename;
            var pageFormatter = new BasePageFormatter();

            var buffer1 = _bufferManager.PinNew(filename, pageFormatter);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.AreEqual(2, _bufferManager.Available);

            var buffer2 = _bufferManager.Pin(buffer1.Block);

            Assert.IsTrue(buffer2.IsPinned);
            Assert.AreEqual(2, _bufferManager.Available);

            _bufferManager.Unpin(buffer1);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.AreEqual(2, _bufferManager.Available);

            _bufferManager.Unpin(buffer2);

            Assert.IsFalse(buffer2.IsPinned);
            Assert.AreEqual(3, _bufferManager.Available);
        }

        [Test]
        public void CanPinBufferWriteAndFlush()
        {
            _poolStrategy = new LRUBufferPoolStrategy(_logManager, _fileManager, 3);
            _bufferManager = new BufferManager(_fileManager, _logManager, _poolStrategy, null, null);

            var filename = RandomFilename;
            var pageFormatter = new BasePageFormatter();

            var buffer1 = _bufferManager.PinNew(filename, pageFormatter);

            buffer1.SetInt(0, 123, 1, 1);
            _bufferManager.FlushAll(1);
            _bufferManager.Unpin(buffer1);

            var page = _fileManager.ResolvePage();
            var canRead = page.Read(new Block(filename, 0));

            var canGetValue = page.GetInt(0, out var value);

            Assert.IsFalse(buffer1.IsPinned);
            Assert.IsTrue(canRead);
            Assert.IsTrue(canGetValue);
            Assert.AreEqual(123, value);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
