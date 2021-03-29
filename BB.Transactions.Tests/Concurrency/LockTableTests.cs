using BB.IO;
using BB.IO.Abstract;
using BB.Memory.Abstract;
using BB.Memory.Logger;
using NUnit.Framework;
using System;

namespace BB.Transactions.Tests.Concurrency
{
    public class LockTableTests
    {
        private ILogManager _logManager;
        private IFileManager _fileManager;

        [SetUp]
        public void Setup()
        {
            _fileManager = new FileManager("temp", "DBs", 100);
            _logManager = new LogManager(_fileManager, RandomFilename);
        }

        [Test]
        public void CanCreateRecoveryManager()
        {

        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
