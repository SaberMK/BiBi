using BB.Record.Base;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Tests.Record.Base
{
    [TestFixture]
    public class SchemaTests
    {
        [Test]
        public void CanCreateSchema()
        {
            Assert.DoesNotThrow(() =>
            {
                var schema = new Schema();
            });
        }

        [Test]
        public void CanAddField()
        {
            var schema = new Schema();
            Assert.DoesNotThrow(() =>
            {
                schema.AddField("field", FieldType.Integer, 0);
            });

            var fields = schema.Fields;
            var field = fields.First();

            Assert.IsNotNull(fields);
            Assert.AreEqual(1, fields.Count);
            Assert.AreEqual(FieldType.Integer, field.Value.Type);
            Assert.AreEqual("field", field.Key);
        }

        [Test]
        public void CanAddIntField()
        {
            var schema = new Schema();

            Assert.DoesNotThrow(() =>
            {
                schema.AddIntField("field");
            });

            var fields = schema.Fields;
            var field = fields.First();

            Assert.IsNotNull(fields);
            Assert.AreEqual(1, fields.Count);
            Assert.AreEqual(FieldType.Integer, field.Value.Type);
            Assert.AreEqual("field", field.Key);
        }

        [Test]
        public void CanAddByteField()
        {
            var schema = new Schema();

            Assert.DoesNotThrow(() =>
            {
                schema.AddByteField("field");
            });

            var fields = schema.Fields;
            var field = fields.First();

            Assert.IsNotNull(fields);
            Assert.AreEqual(1, fields.Count);
            Assert.AreEqual(FieldType.Byte, field.Value.Type);
            Assert.AreEqual("field", field.Key);
        }

        [Test]
        public void CanAddBoolField()
        {
            var schema = new Schema();

            Assert.DoesNotThrow(() =>
            {
                schema.AddBoolField("field");
            });

            var fields = schema.Fields;
            var field = fields.First();

            Assert.IsNotNull(fields);
            Assert.AreEqual(1, fields.Count);
            Assert.AreEqual(FieldType.Bool, field.Value.Type);
            Assert.AreEqual("field", field.Key);
        }

        [Test]
        public void CanAddBlobField()
        {
            var schema = new Schema();

            Assert.DoesNotThrow(() =>
            {
                schema.AddBlobField("field", 50);
            });

            var fields = schema.Fields;
            var field = fields.First();

            Assert.IsNotNull(fields);
            Assert.AreEqual(1, fields.Count);
            Assert.AreEqual(FieldType.Blob, field.Value.Type);
            Assert.AreEqual(50, field.Value.Length);
            Assert.AreEqual("field", field.Key);
        }

        [Test]
        public void CanAddStringField()
        {
            var schema = new Schema();

            Assert.DoesNotThrow(() =>
            {
                schema.AddStringField("field", 50);
            });

            var fields = schema.Fields;
            var field = fields.First();

            Assert.IsNotNull(fields);
            Assert.AreEqual(1, fields.Count);
            Assert.AreEqual(FieldType.String, field.Value.Type);
            Assert.AreEqual("field", field.Key);
            Assert.AreEqual(50, field.Value.Length);
        }

        [Test]
        public void CanAddDateField()
        {
            var schema = new Schema();

            Assert.DoesNotThrow(() =>
            {
                schema.AddDateField("field");
            });

            var fields = schema.Fields;
            var field = fields.First();

            Assert.IsNotNull(fields);
            Assert.AreEqual(1, fields.Count);
            Assert.AreEqual(FieldType.Date, field.Value.Type);
            Assert.AreEqual("field", field.Key);
        }

        [Test]
        public void CanAddFieldFromSchema()
        {
            var schema = new Schema();
            var newSchema = new Schema();

            Assert.DoesNotThrow(() =>
            {
                schema.AddBoolField("field");

                newSchema.Add("field", schema);
            });

            var fields = newSchema.Fields;
            var field = fields.First();

            Assert.IsNotNull(fields);
            Assert.AreEqual(1, fields.Count);
            Assert.AreEqual(FieldType.Bool, field.Value.Type);
            Assert.AreEqual("field", field.Key);
        }

        [Test]
        public void CanAddAllFieldsFromSchema()
        {
            var schema = new Schema();
            var newSchema = new Schema();

            Assert.DoesNotThrow(() =>
            {
                schema.AddBoolField("field1");
                schema.AddByteField("field2");

                newSchema.AddAll(schema);
            });

            var fields = newSchema.Fields;

            var field1 = fields.First();
            var field2 = fields.Last();

            Assert.IsNotNull(fields);
            Assert.AreEqual(2, fields.Count);
            Assert.AreEqual(FieldType.Bool, field1.Value.Type);
            Assert.AreEqual("field1", field1.Key);
            Assert.AreEqual(FieldType.Byte, field2.Value.Type);
            Assert.AreEqual("field2", field2.Key);
        }

        [Test]
        public void CanCheckWhichFieldsArePresented()
        {
            var schema = new Schema();

            Assert.DoesNotThrow(() =>
            {
                schema.AddBoolField("field");
            });

            bool hasField = schema.HasField("field");
            bool noField = schema.HasField("random name");

            Assert.IsTrue(hasField);
            Assert.IsFalse(noField);
        }

        [Test]
        public void CanCheckFieldType()
        {
            var schema = new Schema();

            Assert.DoesNotThrow(() =>
            {
                schema.AddBoolField("field");
            });

            var type = schema.FieldType("field");
            Assert.AreEqual(FieldType.Bool, type);
        }

        [Test]
        public void CanCheckFieldLength()
        {
            var schema = new Schema();

            Assert.DoesNotThrow(() =>
            {
                schema.AddStringField("field", 50);
            });

            var length= schema.Length("field");
            Assert.AreEqual(50, length);
        }
    }
}
