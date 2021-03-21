using BB.IO;
using BB.IO.Abstract;
using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Memory.Logger;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Text;

namespace BB.Memory.Tests.Logger
{
    public class LogEnumeratorTests
    {
        private IFileManager _fileManager;
        private ILogManager _logManager;
        private IEnumerator<BasicLogRecord> _enumerator;

        [SetUp]
        public void Setup()
        {
            _fileManager = new FileManager("temp", "DBs", 100);
            _logManager = new LogManager(_fileManager, RandomFilename);
        }

        [Test]
        public void CanCreateEnumerator()
        {
            Assert.DoesNotThrow(() =>
            {
                _enumerator = _logManager.GetEnumerator();
            });
        }

        [Test]
        public void CanReadLogEntry()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);
            var canAppend = _logManager.Append(new object[] { 123 }, out var lsn);
            var canAppend2 = _logManager.Append(new object[] { 12 }, out var lsn2);

            _enumerator = _logManager.GetEnumerator();
            var entry = _enumerator.Current;
            var canReadLogEntry = entry.NextInt(out var result);
            var canReadLogEntry2 = entry.NextInt(out var result2);

        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
