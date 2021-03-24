using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers.Formatters;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using NUnit.Framework;
using System.IO;
using Guid = System.Guid;

namespace BB.Memory.Tests.Buffers.Strategies
{
    // TODO Add tests according to this buffer only!
    // i.e. test that always selects first unpinned, etc.
    public class NaiveBufferStrategyTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private IBufferPoolStrategy _poolStrategy;

        [SetUp]
        public void Setup()
        {
            _fileManager = new FileManager("temp", "DBs", 100);
            _logManager = new LogManager(_fileManager, RandomFilename);
        }

        [Test]
        public void CanCreateNaiveBufferPoolStrategy()
        {
            Assert.DoesNotThrow(() =>
            {
                _poolStrategy = new NaiveBufferPoolStrategy(_logManager, _fileManager, 3);
            });
        }

        [Test]
        public void CanPinBuffer()
        {
            _poolStrategy = new NaiveBufferPoolStrategy(_logManager, _fileManager, 3);

            var filename = RandomFilename;

            var block = new Block(filename, 0);
            var buffer = _poolStrategy.Pin(block);

            Assert.IsTrue(buffer.IsPinned);
            Assert.AreEqual(filename, buffer.Block.Filename);
            Assert.AreEqual(0, buffer.Block.Id);
            Assert.AreEqual(2, _poolStrategy.Available);
        }

        [Test]
        public void CanPinSameBufferMultipleTimes()
        {
            _poolStrategy = new NaiveBufferPoolStrategy(_logManager, _fileManager, 3);

            var filename = RandomFilename;

            var block = new Block(filename, 0);
            var buffer1 = _poolStrategy.Pin(block);
            var buffer2 = _poolStrategy.Pin(block);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.IsTrue(buffer2.IsPinned);
            Assert.AreEqual(filename, buffer1.Block.Filename);
            Assert.AreEqual(0, buffer1.Block.Id);
            Assert.AreEqual(filename, buffer2.Block.Filename);
            Assert.AreEqual(0, buffer2.Block.Id);
            Assert.AreEqual(2, _poolStrategy.Available);
        }

        [Test]
        public void CanPinAllBuffers()
        {
            _poolStrategy = new NaiveBufferPoolStrategy(_logManager, _fileManager, 3);

            var filename = RandomFilename;

            var block1 = new Block(filename, 0);
            var block2 = new Block(filename, 1);
            var block3 = new Block(filename, 2);
            var buffer1 = _poolStrategy.Pin(block1);
            var buffer2 = _poolStrategy.Pin(block2);
            var buffer3 = _poolStrategy.Pin(block3);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.IsTrue(buffer2.IsPinned);
            Assert.IsTrue(buffer3.IsPinned);
            Assert.AreEqual(filename, buffer1.Block.Filename);
            Assert.AreEqual(0, buffer1.Block.Id);
            Assert.AreEqual(filename, buffer2.Block.Filename);
            Assert.AreEqual(1, buffer2.Block.Id);
            Assert.AreEqual(filename, buffer3.Block.Filename);
            Assert.AreEqual(2, buffer3.Block.Id);
            Assert.AreEqual(0, _poolStrategy.Available);
        }

        [Test]
        public void CannotPinMoreBuffersThatIsOnPool()
        {
            _poolStrategy = new NaiveBufferPoolStrategy(_logManager, _fileManager, 3);

            var filename = RandomFilename;

            var block1 = new Block(filename, 0);
            var block2 = new Block(filename, 1);
            var block3 = new Block(filename, 2);
            var block4 = new Block(filename, 3);
            var buffer1 = _poolStrategy.Pin(block1);
            var buffer2 = _poolStrategy.Pin(block2);
            var buffer3 = _poolStrategy.Pin(block3);
            var buffer4 = _poolStrategy.Pin(block4);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.IsTrue(buffer2.IsPinned);
            Assert.IsTrue(buffer3.IsPinned);
            Assert.AreEqual(filename, buffer1.Block.Filename);
            Assert.AreEqual(0, buffer1.Block.Id);
            Assert.AreEqual(filename, buffer2.Block.Filename);
            Assert.AreEqual(1, buffer2.Block.Id);
            Assert.AreEqual(filename, buffer3.Block.Filename);
            Assert.AreEqual(2, buffer3.Block.Id);
            Assert.IsNull(buffer4);
            Assert.AreEqual(0, _poolStrategy.Available);
        }

        [Test]
        public void CanPinNewBuffer()
        {
            _poolStrategy = new NaiveBufferPoolStrategy(_logManager, _fileManager, 3);

            var filename = RandomFilename;
            var pageFormatter = new BasePageFormatter();
            var buffer1 = _poolStrategy.PinNew(filename, pageFormatter);
            var buffer2 = _poolStrategy.PinNew(filename, pageFormatter);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.IsTrue(buffer2.IsPinned);
            Assert.AreEqual(filename, buffer1.Block.Filename);
            Assert.AreEqual(0, buffer1.Block.Id);
            Assert.AreEqual(filename, buffer2.Block.Filename);
            Assert.AreEqual(1, buffer2.Block.Id);
            Assert.AreEqual(1, _poolStrategy.Available);
        }



        [Test]
        public void CannotPinMoreNewBlocksThatPoolHave()
        {
            _poolStrategy = new NaiveBufferPoolStrategy(_logManager, _fileManager, 3);

            var filename = RandomFilename;
            var pageFormatter = new BasePageFormatter();

            var buffer1 = _poolStrategy.PinNew(filename, pageFormatter);
            var buffer2 = _poolStrategy.PinNew(filename, pageFormatter);
            var buffer3 = _poolStrategy.PinNew(filename, pageFormatter);
            var buffer4 = _poolStrategy.PinNew(filename, pageFormatter);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.IsTrue(buffer2.IsPinned);
            Assert.IsTrue(buffer3.IsPinned);
            Assert.AreEqual(filename, buffer1.Block.Filename);
            Assert.AreEqual(0, buffer1.Block.Id);
            Assert.AreEqual(filename, buffer2.Block.Filename);
            Assert.AreEqual(1, buffer2.Block.Id);
            Assert.AreEqual(filename, buffer3.Block.Filename);
            Assert.AreEqual(2, buffer3.Block.Id);
            Assert.IsNull(buffer4);
            Assert.AreEqual(0, _poolStrategy.Available);
        }

        [Test]
        public void CanUnpinBuffer()
        {
            _poolStrategy = new NaiveBufferPoolStrategy(_logManager, _fileManager, 3);

            var filename = RandomFilename;
            var pageFormatter = new BasePageFormatter();
            var buffer1 = _poolStrategy.PinNew(filename, pageFormatter);
            var buffer2 = _poolStrategy.PinNew(filename, pageFormatter);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.IsTrue(buffer2.IsPinned);
            Assert.AreEqual(filename, buffer1.Block.Filename);
            Assert.AreEqual(0, buffer1.Block.Id);
            Assert.AreEqual(filename, buffer2.Block.Filename);
            Assert.AreEqual(1, buffer2.Block.Id);
            Assert.AreEqual(1, _poolStrategy.Available);

            _poolStrategy.Unpin(buffer1);
            Assert.IsFalse(buffer1.IsPinned);
            Assert.AreEqual(2, _poolStrategy.Available);

            _poolStrategy.Unpin(buffer2);
            Assert.IsFalse(buffer2.IsPinned);
            Assert.AreEqual(3, _poolStrategy.Available);
        }

        [Test]
        public void CanPinBufferMultipleTimesAndUnpinMultipleTimes()
        {
            _poolStrategy = new NaiveBufferPoolStrategy(_logManager, _fileManager, 3);

            var filename = RandomFilename;
            var pageFormatter = new BasePageFormatter();

            var buffer1 = _poolStrategy.PinNew(filename, pageFormatter);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.AreEqual(2, _poolStrategy.Available);

            var buffer2 = _poolStrategy.Pin(buffer1.Block);

            Assert.IsTrue(buffer2.IsPinned);
            Assert.AreEqual(2, _poolStrategy.Available);

            _poolStrategy.Unpin(buffer1);

            Assert.IsTrue(buffer1.IsPinned);
            Assert.AreEqual(2, _poolStrategy.Available);

            _poolStrategy.Unpin(buffer2);

            Assert.IsFalse(buffer2.IsPinned);
            Assert.AreEqual(3, _poolStrategy.Available);
        }

        [Test]
        public void CanPinBufferWriteAndFlush()
        {
            _poolStrategy = new NaiveBufferPoolStrategy(_logManager, _fileManager, 3);

            var filename = RandomFilename;
            var pageFormatter = new BasePageFormatter();

            var buffer1 = _poolStrategy.PinNew(filename, pageFormatter);

            buffer1.SetInt(0, 123, 1, 1);
            _poolStrategy.Unpin(buffer1);
            _poolStrategy.FlushAll(1);

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
