using BB.IO;
using BB.IO.Abstract;
using BB.Memory.Abstract;
using BB.Record.Base;
using BB.Record.Entity;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Record.Tests.Entity
{
    [TestFixture]
    public class RecordFormatterTests
    {
        private IFileManager _fileManager;
        private TableInfo _tableInfo;
        private string _logName;
        private IPageFormatter _recordFormatter;
        private Schema _schema;

        [SetUp]
        public void Setup()
        {
            _logName = RandomFilename;
            _fileManager = new FileManager("temp", "DBs", 200);
            _schema = new Schema();
        }

        [Test]
        public void CanCreateRecordFormatter()
        {
            Assert.DoesNotThrow(() =>
            {
                _recordFormatter = new RecordFormatter()
            });
        }

        [OneTimeTearDown]
        public void ClearDirectory()
        {
            Directory.Delete("DBs", true);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
