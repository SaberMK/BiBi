using BB.Record.Base;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Record.Tests.Base
{
    [TestFixture]
    public class TableInfoTests
    {
        [Test]
        public void CanCreateTableInfo_v1()
        {
            var schema = new Schema();
            schema.AddIntField("field");

            Assert.DoesNotThrow(() =>
            {
                var tableInfo = new TableInfo("table", schema);
            });
        }

        [Test]
        public void CanCreateTableInfo_v2()
        {
            var schema = new Schema();
            schema.AddIntField("field");

            var offsets = new Dictionary<string, int>
            {
                { "field", 4 }
            };

            Assert.DoesNotThrow(() =>
            {
                var tableInfo = new TableInfo("table", schema, offsets, 8);
            });
        }

        [Test]
        public void CanGetTableFilename()
        {
            var schema = new Schema();
            schema.AddIntField("field");

            var tableInfo = new TableInfo("table", schema);

            Assert.AreEqual("table.tbl", tableInfo.Filename);
        }


        [Test]
        public void CanGetSchema()
        {
            var schema = new Schema();
            schema.AddIntField("field");

            var tableInfo = new TableInfo("table", schema);

            Assert.AreEqual(schema, tableInfo.Schema);
        }


        [Test]
        public void CanGetLength()
        {
            var schema = new Schema();
            schema.AddIntField("field");

            var tableInfo = new TableInfo("table", schema);

            Assert.AreEqual(4, tableInfo.RecordLength);
        }

        [Test]
        public void CanGetOffset()
        {
            var schema = new Schema();
            schema.AddIntField("field");

            var tableInfo = new TableInfo("table", schema);

            var offset = tableInfo.Offset("field");

            Assert.AreEqual(0, offset);
        }

        [Test]
        public void CannotGetBadOffset()
        {
            var schema = new Schema();
            schema.AddIntField("field");

            var tableInfo = new TableInfo("table", schema);

            var offset = tableInfo.Offset("field123");

            Assert.AreEqual(-1, offset);
        }

        [Test]
        public void CanCreateCompleteRecord()
        {
            var schema = new Schema();
            schema.AddIntField("field1");
            schema.AddBoolField("field2");
            schema.AddByteField("field3");
            schema.AddBlobField("field4", 10);
            schema.AddStringField("field5", 10);
            schema.AddDateField("field6");

            var tableInfo = new TableInfo("table", schema);

            Assert.AreEqual(42, tableInfo.RecordLength);
        }


        [Test]
        public void CannotCreateCompleteRecordWithBrokenField()
        {
            var schema = new Schema();
            schema.AddField("field1", (FieldType)8, 0);

            var tableInfo = new TableInfo("table", schema);

            Assert.AreEqual(-1, tableInfo.RecordLength);
        }
    }
}
