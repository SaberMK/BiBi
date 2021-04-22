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

namespace BB.Tests.Record.Entity
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
            _tableInfo = new TableInfo(RandomFilename, _schema);
            Assert.DoesNotThrow(() =>
            {
                _recordFormatter = new RecordFormatter(_tableInfo, _fileManager);
            });
        }

        [Test]
        public void CanFormatPageWithNoRecordsInside()
        {
            _tableInfo = new TableInfo(RandomFilename, _schema);
            _recordFormatter = new RecordFormatter(_tableInfo, _fileManager);
            var page = _fileManager.ResolvePage();

            Assert.DoesNotThrow(() =>
            {
                _recordFormatter.Format(page);
            });
        }

        [Test]
        public void CanFormatPageWithOneIntFieldInside()
        {
            _schema.AddIntField("field");

            _tableInfo = new TableInfo(RandomFilename, _schema);
            _recordFormatter = new RecordFormatter(_tableInfo, _fileManager);

            var page = _fileManager.ResolvePage();

            Assert.DoesNotThrow(() =>
            {
                _recordFormatter.Format(page);
            });
        }

        [Test]
        public void CanFormatPageWithOneBoolFieldInside()
        {
            _schema.AddBoolField("field");

            _tableInfo = new TableInfo(RandomFilename, _schema);
            _recordFormatter = new RecordFormatter(_tableInfo, _fileManager);

            var page = _fileManager.ResolvePage();

            Assert.DoesNotThrow(() =>
            {
                _recordFormatter.Format(page);
            });
        }


        [Test]
        public void CanFormatPageWithOneByteFieldInside()
        {
            _schema.AddByteField("field");

            _tableInfo = new TableInfo(RandomFilename, _schema);
            _recordFormatter = new RecordFormatter(_tableInfo, _fileManager);

            var page = _fileManager.ResolvePage();

            Assert.DoesNotThrow(() =>
            {
                _recordFormatter.Format(page);
            });
        }


        [Test]
        public void CanFormatPageWithOneBlobFieldInside()
        {
            _schema.AddBlobField("field", 30);

            _tableInfo = new TableInfo(RandomFilename, _schema);
            _recordFormatter = new RecordFormatter(_tableInfo, _fileManager);

            var page = _fileManager.ResolvePage();

            Assert.DoesNotThrow(() =>
            {
                _recordFormatter.Format(page);
            });
        }

        [Test]
        public void CanFormatPageWithOneStringFieldInside()
        {
            _schema.AddStringField("field", 30);

            _tableInfo = new TableInfo(RandomFilename, _schema);
            _recordFormatter = new RecordFormatter(_tableInfo, _fileManager);

            var page = _fileManager.ResolvePage();

            Assert.DoesNotThrow(() =>
            {
                _recordFormatter.Format(page);
            });
        }

        [Test]
        public void CanFormatPageWithOneDateFieldInside()
        {
            _schema.AddDateField("field");

            _tableInfo = new TableInfo(RandomFilename, _schema);
            _recordFormatter = new RecordFormatter(_tableInfo, _fileManager);

            var page = _fileManager.ResolvePage();

            Assert.DoesNotThrow(() =>
            {
                _recordFormatter.Format(page);
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
