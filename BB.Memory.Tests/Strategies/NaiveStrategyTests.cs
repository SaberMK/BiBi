using BB.IO;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Log;
using NUnit.Framework;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace BB.Memory.Tests.Strategies
{
    public class NaiveStrategyTests
    {
        private IBufferStrategy _naiveStrategy;
        private string blocksFilename1;
        private string blocksFilename2;

        [SetUp]
        public void SetUp()
        {
            //TOOD Mock logs so they would not write to real folders


            var directoryManager = new DirectoryManager("Logs", 30);
            var logManager = new LogManager(directoryManager.GetManager(GetRandomFilename()));

            _naiveStrategy = new NaiveBufferStrategy(directoryManager, logManager, 6);
        }

        [Test]
        public void CanPinOnNaiveStrategy()
        {
            blocksFilename1 = GetRandomFilename();
            var buffer = _naiveStrategy.Pin(new Block(0, blocksFilename1));
            Assert.True(buffer.IsPinned);
            Assert.AreEqual(5, _naiveStrategy.Available);
        }

        [Test]
        public void CanUnpinOnNaiveStrategy()
        {
            blocksFilename1 = GetRandomFilename();
            var buffer = _naiveStrategy.Pin(new Block(0, blocksFilename1));
            Assert.True(buffer.IsPinned);
            Assert.AreEqual(5, _naiveStrategy.Available);
            _naiveStrategy.Unpin(buffer);
            Assert.False(buffer.IsPinned);
            Assert.AreEqual(6, _naiveStrategy.Available);
        }

        [Test]
        public void CanPinAlreadyPinnedBlockOnNaiveStrategy()
        {
            blocksFilename1 = GetRandomFilename();
            var buffer = _naiveStrategy.Pin(new Block(0, blocksFilename1));

            Assert.True(buffer.IsPinned);
            Assert.AreEqual(5, _naiveStrategy.Available);

            var buffer2 = _naiveStrategy.Pin(new Block(0, blocksFilename1));

            Assert.True(buffer2.IsPinned);
            Assert.AreEqual(5, _naiveStrategy.Available);
        }

        [Test]
        public void CannotUnpinUnpinnedBlockOnNaiveStrategy()
        {
            blocksFilename1 = GetRandomFilename();
            var buffer = _naiveStrategy.Pin(new Block(0, blocksFilename1));
            _naiveStrategy.Unpin(buffer);

            Assert.False(buffer.IsPinned);
            Assert.AreEqual(6, _naiveStrategy.Available);

            _naiveStrategy.Unpin(buffer);

            Assert.AreEqual(6, _naiveStrategy.Available);
        }

        [Test]
        public void CanPinAllBuffersOnNaiveStrategy()
        {
            blocksFilename1 = GetRandomFilename();
            var buffers = new Buffer[6];
            for (int i = 0; i < 6; ++i)
            {
                buffers[i] = _naiveStrategy.Pin(new Block(i, blocksFilename1));
            }

            for (int i = 0; i < 6; ++i)
            {
                Assert.True(buffers[i].IsPinned);
            }

            Assert.AreEqual(0, _naiveStrategy.Available);
        }

        [Test]
        public void CannotPinMoreThanBufferCapacityOnNaiveStrategy()
        {
            blocksFilename1 = GetRandomFilename();
            var buffers = new Buffer[6];
            for (int i = 0; i < 6; ++i)
            {
                buffers[i] = _naiveStrategy.Pin(new Block(i, blocksFilename1));
            }

            for (int i = 0; i < 6; ++i)
            {
                Assert.True(buffers[i].IsPinned);
            }

            Assert.AreEqual(0, _naiveStrategy.Available);

            var overflowBuffer = _naiveStrategy.Pin(new Block(7, blocksFilename1));
            Assert.IsNull(overflowBuffer);
        }

        [Test]
        public void AlwaysChangesFirstPossibleBlock()
        {
            blocksFilename1 = GetRandomFilename();
            var buffers = new Buffer[6];

            buffers[0] = _naiveStrategy.Pin(new Block(0, blocksFilename1));
            Assert.True(_naiveStrategy.Buffers.ElementAt(0).IsPinned);

            buffers[1] = _naiveStrategy.Pin(new Block(1, blocksFilename1));
            Assert.True(_naiveStrategy.Buffers.ElementAt(1).IsPinned);

            _naiveStrategy.Unpin(buffers[0]);
            Assert.False(_naiveStrategy.Buffers.ElementAt(0).IsPinned);
            Assert.True(_naiveStrategy.Buffers.ElementAt(1).IsPinned);

            buffers[2] = _naiveStrategy.Pin(new Block(2, blocksFilename1));
            Assert.True(_naiveStrategy.Buffers.ElementAt(0).IsPinned);
            Assert.True(_naiveStrategy.Buffers.ElementAt(1).IsPinned);
        }


        private string GetRandomFilename() => $"{System.Guid.NewGuid()}.tmp";

        [TearDown]
        public void Teardown()
        {

        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            try
            {
                if (Directory.Exists("Logs"))
                {
                    Directory.Delete("Logs", true);
                }
            }
            catch(IOException)
            {

            }
        }
    }
}
