using BB.IO;
using BB.IO.Abstract;
using NUnit.Framework;
using System;
using System.IO;

namespace BB.Tests.Io
{
    public class FileManagerTests
    {
        private IFileManager _fileManager;

        [SetUp]
        public void SetUp()
        {
            if (Directory.Exists("DBs"))
            {
                Directory.Delete("DBs", true);
            }

            _fileManager = new FileManager("temp", "DBs", 100);
        }

        [Test]
        public void DbFileExistsOnNewDirectory()
        {
            Assert.IsTrue(Directory.Exists("DBs/temp"));
            Assert.IsTrue(_fileManager.IsNew);
        }

        [Test]
        public void BlockSizeIsCorrect()
        {
            Assert.AreEqual(100, _fileManager.BlockSize);
        }

        [Test]
        public void CanAppendMemoryBlock()
        {
            var filename = RandomFilename;
            var canAppend = _fileManager.Append(filename, out var block);

            Assert.True(canAppend);
            Assert.AreEqual(0, block.Id);
            Assert.AreEqual(filename, block.Filename);
        }

        [Test]
        public void CanWriteMemoryBlock()
        {
            var filename = RandomFilename;
            var canAppend = _fileManager.Append(filename, out var block);

            var buffer = new byte[_fileManager.BlockSize];
            buffer[0] = 1;
            var canWrite = _fileManager.Write(block, buffer);

            _fileManager.Dispose();

            var readData = File.ReadAllBytes($"DBs/temp/{filename}");

            Assert.True(canAppend);
            Assert.True(canWrite);
            Assert.AreEqual(0, block.Id);
            Assert.AreEqual(filename, block.Filename);

            for (int i = 0; i < _fileManager.BlockSize; ++i)
            {
                Assert.AreEqual(readData[i], buffer[i]);
            }
        }

        [Test]
        public void CanReadFirstMemoryBlock()
        {
            var filename = RandomFilename;
            var canAppend = _fileManager.Append(filename, out var block);

            var buffer = new byte[_fileManager.BlockSize];
            buffer[0] = 1;
            var canWrite = _fileManager.Write(block, buffer);

            _fileManager.Dispose();

            _fileManager = new FileManager("temp", "DBs", 100);
            var readResult = _fileManager.Read(block, out var buff);

            Assert.True(canAppend);
            Assert.True(canWrite);
            Assert.True(readResult);
            Assert.AreEqual(0, block.Id);
            Assert.AreEqual(filename, block.Filename);

            for (int i = 0; i < _fileManager.BlockSize; ++i)
            {
                Assert.AreEqual(buffer[i], buff[i]);
            }
        }

        [Test]
        public void CanReadSeveralMemoryBlocks()
        {
            var filename = RandomFilename;
            var canAppend1 = _fileManager.Append(filename, out var block1);
            var canAppend2 = _fileManager.Append(filename, out var block2);

            var buffer1 = new byte[_fileManager.BlockSize];
            buffer1[0] = 1;
            var canWrite1 = _fileManager.Write(block1, buffer1);

            var buffer2 = new byte[_fileManager.BlockSize];
            buffer2[0] = 2;
            var canWrite2 = _fileManager.Write(block2, buffer2);

            _fileManager.Dispose();

            _fileManager = new FileManager("temp", "DBs", 100);
            var readResult1 = _fileManager.Read(block1, out var buff1);
            var readResult2 = _fileManager.Read(block2, out var buff2);

            Assert.True(canAppend1);
            Assert.True(canAppend2);
            Assert.True(canWrite1);
            Assert.True(canWrite2);
            Assert.True(readResult1);
            Assert.True(readResult2);
            Assert.AreEqual(0, block1.Id);
            Assert.AreEqual(filename, block1.Filename);
            Assert.AreEqual(1, block2.Id);
            Assert.AreEqual(filename, block2.Filename);

            for (int i = 0; i < _fileManager.BlockSize; ++i)
            {
                Assert.AreEqual(buffer1[i], buff1[i]);
                Assert.AreEqual(buffer2[i], buff2[i]);
            }
        }

        [Test]
        public void LengthOfAppendedNewFileEqualsToBlockSize()
        {
            var filename = RandomFilename;
            var canAppend = _fileManager.Append(filename, out var block);

            var length = _fileManager.Length(filename);

            Assert.True(canAppend);
            Assert.AreEqual(length, _fileManager.BlockSize);
        }

        [Test]
        public void CannotReadInABadRange()
        {
            var filename = RandomFilename;
            var canAppend = _fileManager.Append(filename, out var block);

            var canRead1 = _fileManager.Read(new IO.Primitives.Block(filename, -1), out var buff1);
            var canRead2 = _fileManager.Read(new IO.Primitives.Block(filename, 100), out var buff2);

            Assert.True(canAppend);
            Assert.False(canRead1);
            Assert.False(canRead2);
            Assert.AreEqual(default(byte[]), buff1);
            Assert.AreEqual(default(byte[]), buff2);
        }

        [Test]
        public void CannotWriteInABadRange()
        {
            var filename = RandomFilename;
            var canAppend = _fileManager.Append(filename, out var block);

            var buff = new byte[_fileManager.BlockSize];

            var canWrite1 = _fileManager.Write(new IO.Primitives.Block(filename, -1), buff);
            var canWrite2 = _fileManager.Write(new IO.Primitives.Block(filename, 100), buff);

            Assert.True(canAppend);
            Assert.False(canWrite1);
            Assert.False(canWrite2);
        }

        [Test]
        public void CanDisposeFileManagerWithOpenedFiles()
        {
            Assert.DoesNotThrow(() =>
            {
                var filename = RandomFilename;
                var canAppend = _fileManager.Append(filename, out var block);
                _fileManager.Dispose();
            });
        }

        [Test]
        public void CanGetLastBlockId()
        {
            var filename = RandomFilename;
            var lastBlockId1 = _fileManager.LastBlockId(filename);

            var canAppend1 = _fileManager.Append(filename, out var _);
            var lastBlockId2 = _fileManager.LastBlockId(filename);

            var canAppend2 = _fileManager.Append(filename, out var _);
            var lastBlockId3 = _fileManager.LastBlockId(filename);

            Assert.True(canAppend1);
            Assert.True(canAppend2);
            Assert.AreEqual(0, lastBlockId1);
            Assert.AreEqual(0, lastBlockId2);
            Assert.AreEqual(1, lastBlockId3);
        }

        [TearDown]
        public void TearDown()
        {
            // Not using ?. because code coverage doesn't like it
            if (_fileManager != null)
                _fileManager.Dispose();
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
