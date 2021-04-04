using BB.IO;
using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Memory.Buffers;
using BB.Memory.Buffers.Formatters;
using BB.Memory.Buffers.Strategies;
using BB.Memory.Logger;
using BB.Transactions.Concurrency;
using BB.Transactions.Helpers;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Text;

namespace BB.Transactions.Tests.Helpers
{
    public class TransactionBufferListHelper
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;
        private IBufferManager _bufferManager;
        private TransactionBuffersList _buffersList;

        [SetUp]
        public void Setup()
        {
            _fileManager = new FileManager("temp", "DBs", 100);
            _logManager = new LogManager(_fileManager, RandomFilename);
            _bufferManager = new BufferManager(_fileManager, _logManager, new NaiveBufferPoolStrategy(_logManager, _fileManager, 3), TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(300));
        }

        [Test]
        public void CanCreateTransactionBuffersList()
        {
            Assert.DoesNotThrow(() =>
            {
                _buffersList = new TransactionBuffersList(_bufferManager);
            });
        }

        [Test]
        public void CanPinBuffer()
        {
            _buffersList = new TransactionBuffersList(_bufferManager);

            _ = _fileManager.Append(RandomFilename, out var block);

            Assert.DoesNotThrow(() =>
            {
                _bufferManager.Pin(block);
            });
        }

        [Test]
        public void CanPinNewBuffer()
        {
            _buffersList = new TransactionBuffersList(_bufferManager);

            Assert.DoesNotThrow(() =>
            {
                _buffersList.PinNew(RandomFilename, new BasePageFormatter());
            });
        }

        [Test]
        public void CanGetTransactionBuffer()
        {
            _buffersList = new TransactionBuffersList(_bufferManager);

            _ = _fileManager.Append(RandomFilename, out var block);

            Assert.DoesNotThrow(() =>
            {
                _buffersList.Pin(block);
            });

            var buffer = _buffersList.GetBuffer(block);
            Assert.IsNotNull(buffer);
        }

        [Test]
        public void CannotGetUnusedBuffer()
        {
            _buffersList = new TransactionBuffersList(_bufferManager);

            var block = new Block(RandomFilename, 0);

            var buffer = _buffersList.GetBuffer(block);
            Assert.IsNull(buffer);
        }

        [Test]
        public void CanPinMultipleTimes()
        {
            _buffersList = new TransactionBuffersList(_bufferManager);

            var block = new Block(RandomFilename, 0);

            Assert.DoesNotThrow(() =>
            {
                _buffersList.Pin(block);
                _buffersList.Pin(block);
                _buffersList.Pin(block);
            });
        }


        [Test]
        public void CanUnpinMultipleBuffers()
        {
            _buffersList = new TransactionBuffersList(_bufferManager);

            var block = new Block(RandomFilename, 0);

            Assert.DoesNotThrow(() =>
            {
                _buffersList.Pin(block);
                _buffersList.Pin(block);
                _buffersList.Pin(block);
                _buffersList.Unpin(block);
                _buffersList.Unpin(block);
                _buffersList.Unpin(block);
            });
        }

        [Test]
        public void CanUnpinMultipleBuffersByUnpinAll()
        {
            _buffersList = new TransactionBuffersList(_bufferManager);

            var block = new Block(RandomFilename, 0);

            Assert.DoesNotThrow(() =>
            {
                _buffersList.Pin(block);
                _buffersList.Pin(block);
                _buffersList.Pin(block);
                _buffersList.UnpinAll();
            });
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
