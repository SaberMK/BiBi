using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Exceptions;
using BB.Memory.Log;
using NUnit.Framework;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BB.Memory.Tests
{
    public class BufferManagerTests
    {
        private IDirectoryManager _directoryManager;
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private IBufferManager _bufferManager;

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
        public void CanCreateBufferManager()
        {
            Assert.DoesNotThrow(() =>
            {
                _bufferManager = CreateDefaultBufferManager();
            });
        }

        [Test]
        public void GlobalBufferManagerTest()
        {
            _bufferManager = CreateDefaultBufferManager();

            var buffers = new Buffer[6];
            buffers[0] = _bufferManager.Pin(new Block(0, _fileManager.Filename));
            buffers[1] = _bufferManager.Pin(new Block(1, _fileManager.Filename));
            buffers[2] = _bufferManager.Pin(new Block(2, _fileManager.Filename));

            _bufferManager.Unpin(buffers[1]);
            buffers[1] = null;

            buffers[3] = _bufferManager.Pin(new Block(0, _fileManager.Filename));
            buffers[4] = _bufferManager.Pin(new Block(1, _fileManager.Filename));

            var availableBuffers = _bufferManager.Available;

            Assert.Throws<BufferAbortionException>(() =>
            {
                buffers[5] = _bufferManager.Pin(new Block(3, _fileManager.Filename));
            });

            Assert.DoesNotThrow(() =>
            {
                _bufferManager.Unpin(buffers[2]);
                buffers[5] = _bufferManager.Pin(new Block(3, _fileManager.Filename));
            });
        }

        [Test]
        public void GlobalBufferManagerTestWithDifferentFiles()
        {
            _bufferManager = CreateDefaultBufferManager();

            var filename1 = GetRandomFilename();
            var filename2 = GetRandomFilename();

            var buffers = new Buffer[6];
            buffers[0] = _bufferManager.Pin(new Block(0, filename1));
            buffers[1] = _bufferManager.Pin(new Block(1, filename2));
            buffers[2] = _bufferManager.Pin(new Block(2, filename1));

            _bufferManager.Unpin(buffers[1]);
            buffers[1] = null;

            buffers[3] = _bufferManager.Pin(new Block(0, filename1));
            buffers[4] = _bufferManager.Pin(new Block(1, filename1));

            var availableBuffers = _bufferManager.Available;

            Assert.Throws<BufferAbortionException>(() =>
            {
                buffers[5] = _bufferManager.Pin(new Block(3, filename2));
            });

            Assert.DoesNotThrow(() =>
            {
                _bufferManager.Unpin(buffers[2]);
                buffers[5] = _bufferManager.Pin(new Block(3, filename2));
            });
        }

        [TearDown]
        public void TearDown()
        {
            _logManager.Dispose();
            _fileManager.Dispose();
            _directoryManager.Dispose();
            _bufferManager.Dispose();
        }

        private BufferManager CreateDefaultBufferManager() => new BufferManager(_directoryManager, _logManager, StrategyType.Naive, 3, System.TimeSpan.FromMilliseconds(400));
        private string GetRandomFilename() => $"{System.Guid.NewGuid()}.tmp";
    }
}
