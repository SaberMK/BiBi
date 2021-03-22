using BB.IO;
using BB.IO.Abstract;
using BB.Memory.Abstract;
using BB.Memory.Base;
using BB.Memory.Logger;
using NUnit.Framework;
using System;
using System.Collections;
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
        public void CanCreateAndDisposeEnumerator()
        {
            Assert.DoesNotThrow(() =>
            {
                _enumerator = _logManager.GetEnumerator();
                _enumerator.Dispose();
            });
        }

        [Test]
        public void CanReadLogEntriesWithOneValue()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);
            var canAppend = _logManager.Append(new object[] { 123 }, out var lsn);
            var canAppend2 = _logManager.Append(new object[] { 12 }, out var lsn2);

            _enumerator = _logManager.GetEnumerator();
            var entry = _enumerator.Current;
            var canReadLogEntry = entry.NextInt(out var result1);
            var canMove1 = _enumerator.MoveNext();
            var entry2 = _enumerator.Current;
            var canReadLogEntry2 = entry2.NextInt(out var result2);
            var canMove2 = _enumerator.MoveNext();

            _enumerator.Dispose();

            Assert.IsTrue(canAppend);
            Assert.IsTrue(canAppend2);
            Assert.IsTrue(canReadLogEntry);
            Assert.IsTrue(canReadLogEntry2);
            Assert.AreEqual(0, lsn);
            Assert.AreEqual(0, lsn2);

            Assert.IsTrue(canMove1);
            Assert.IsFalse(canMove2);

            Assert.AreEqual(12, result1);
            Assert.AreEqual(123, result2);
        }

        [Test]
        public void CanReadLogEntriesWithMultipleValues()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);
            var canAppend = _logManager.Append(new object[] { 1,2,3,"abc" }, out var lsn);
            var canAppend2 = _logManager.Append(new object[] { 4,5,6,"def" }, out var lsn2);

            _enumerator = _logManager.GetEnumerator();
            var entry = _enumerator.Current;
            var canReadLogEntry1_1 = entry.NextInt(out var result1_1);
            var canReadLogEntry1_2 = entry.NextInt(out var result1_2);
            var canReadLogEntry1_3 = entry.NextInt(out var result1_3);
            var canReadLogEntry1_4 = entry.NextString(out var result1_4);

            var canMove1 = _enumerator.MoveNext();
            var entry2 = _enumerator.Current;
            var canReadLogEntry2_1 = entry2.NextInt(out var result2_1);
            var canReadLogEntry2_2 = entry2.NextInt(out var result2_2);
            var canReadLogEntry2_3 = entry2.NextInt(out var result2_3);
            var canReadLogEntry2_4 = entry2.NextString(out var result2_4);
            var canMove2 = _enumerator.MoveNext();

            _enumerator.Dispose();

            Assert.IsTrue(canAppend);
            Assert.IsTrue(canAppend2);
            Assert.IsTrue(canReadLogEntry1_1);
            Assert.IsTrue(canReadLogEntry1_2);
            Assert.IsTrue(canReadLogEntry1_3);
            Assert.IsTrue(canReadLogEntry1_4);
            Assert.IsTrue(canReadLogEntry2_1);
            Assert.IsTrue(canReadLogEntry2_2);
            Assert.IsTrue(canReadLogEntry2_3);
            Assert.IsTrue(canReadLogEntry2_4);
            Assert.AreEqual(0, lsn);
            Assert.AreEqual(0, lsn2);

            Assert.IsTrue(canMove1);
            Assert.IsFalse(canMove2);

            Assert.AreEqual(4, result1_1);
            Assert.AreEqual(5, result1_2);
            Assert.AreEqual(6, result1_3);
            Assert.AreEqual("def", result1_4);

            Assert.AreEqual(1, result2_1);
            Assert.AreEqual(2, result2_2);
            Assert.AreEqual(3, result2_3);
            Assert.AreEqual("abc", result2_4);
        }
        
        [Test]
        public void CanGetLegacyCSharpEnumerator()
        {
            _enumerator = _logManager.GetEnumerator();
            var obj = ((IEnumerator)(_enumerator)).Current;

            Assert.IsTrue(obj is LogEnumerator);
        }

        [Test]
        public void CanReadLogEntryAndResetAndReadAgain()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);
            var canAppend = _logManager.Append(new object[] { 123 }, out var lsn);

            _enumerator = _logManager.GetEnumerator();
            var entry = _enumerator.Current;
            var canReadLogEntry = entry.NextInt(out var result1);
            var canMove1 = _enumerator.MoveNext();

            _enumerator.Reset();

            var entry2 = _enumerator.Current;
            var canReadLogEntry2 = entry2.NextInt(out var result2);
            var canMove2 = _enumerator.MoveNext();

            _enumerator.Dispose();

            Assert.IsTrue(canAppend);
            Assert.AreEqual(0, lsn);

            Assert.IsTrue(canReadLogEntry);
            Assert.AreEqual(123, result1);
            Assert.IsTrue(canReadLogEntry2);
            Assert.AreEqual(123, result2);

            Assert.IsFalse(canMove1);
            Assert.IsFalse(canMove2);
        }

        [Test]
        public void CannotMoveOutOfBounds()
        {
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);
            var canAppend = _logManager.Append(new object[] { 123 }, out var lsn);

            _enumerator = _logManager.GetEnumerator();
            var entry = _enumerator.Current;
            var canReadLogEntry = entry.NextInt(out var result1);
            var canMove1 = _enumerator.MoveNext();
            var canMove2 = _enumerator.MoveNext();
            var canMove3 = _enumerator.MoveNext();

            _enumerator.Dispose();

            Assert.IsTrue(canAppend);
            Assert.AreEqual(0, lsn);

            Assert.IsTrue(canReadLogEntry);
            Assert.AreEqual(123, result1);

            Assert.IsFalse(canMove1);
            Assert.IsFalse(canMove2);
            Assert.IsFalse(canMove3);
        }

        [Test]
        public void CanStoreLogRecordsOnMultipleMemoryPages()
        { 
            var storageString = "London is the capital of GB";
            var filename = RandomFilename;
            _logManager = new LogManager(_fileManager, filename);
            var canAppe`nd = _logManager.Append(new object[] { 1, 2, storageString }, out var lsn);
            var canAppend2 = _logManager.Append(new object[] { 3, 4, storageString }, out var lsn2);
            var canAppend3 = _logManager.Append(new object[] { 5, 6, storageString }, out var lsn3);

            _enumerator = _logManager.GetEnumerator();
            var entry = _enumerator.Current;
            var canReadLogEntry1_1 = entry.NextInt(out var result1_1);
            var canReadLogEntry1_2 = entry.NextInt(out var result1_2);
            var canReadLogEntry1_3 = entry.NextString(out var result1_3);

            var canMove1 = _enumerator.MoveNext();
            var entry2 = _enumerator.Current;
            var canReadLogEntry2_1 = entry2.NextInt(out var result2_1);
            var canReadLogEntry2_2 = entry2.NextInt(out var result2_2);
            var canReadLogEntry2_3 = entry2.NextString(out var result2_3);

            var canMove2 = _enumerator.MoveNext();
            var entry3 = _enumerator.Current;
            var canReadLogEntry3_1 = entry3.NextInt(out var result3_1);
            var canReadLogEntry3_2 = entry3.NextInt(out var result3_2);
            var canReadLogEntry3_3 = entry3.NextString(out var result3_3);
            var canMove3 = _enumerator.MoveNext();

            _enumerator.Dispose();

            Assert.IsTrue(canAppend);
            Assert.IsTrue(canAppend2);
            Assert.IsTrue(canAppend3);
            Assert.IsTrue(canReadLogEntry1_1);
            Assert.IsTrue(canReadLogEntry1_2);
            Assert.IsTrue(canReadLogEntry1_3);
            Assert.IsTrue(canReadLogEntry2_1);
            Assert.IsTrue(canReadLogEntry2_2);
            Assert.IsTrue(canReadLogEntry2_3);
            Assert.IsTrue(canReadLogEntry3_1);
            Assert.IsTrue(canReadLogEntry3_2);
            Assert.IsTrue(canReadLogEntry3_3);
            Assert.AreEqual(0, lsn);
            Assert.AreEqual(0, lsn2);
            Assert.AreEqual(1, lsn3);

            Assert.IsTrue(canMove1);
            Assert.IsTrue(canMove2);
            Assert.IsFalse(canMove3);

            Assert.AreEqual(5, result1_1);
            Assert.AreEqual(6, result1_2);
            Assert.AreEqual(storageString, result1_3);

            Assert.AreEqual(3, result2_1);
            Assert.AreEqual(4, result2_2);
            Assert.AreEqual(storageString, result2_3);

            Assert.AreEqual(1, result3_1);
            Assert.AreEqual(2, result3_2);
            Assert.AreEqual(storageString, result3_3);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
