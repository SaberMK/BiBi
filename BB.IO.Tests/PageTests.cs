using BB.IO.Primitives;
using NUnit.Framework;
using System;

namespace BB.IO.Tests
{
    public class PageTests
    {
        [Test]
        public void CanCreatePageWithSize()
        {
            var page = new Page(0, 100);

            Assert.IsNotNull(page);
            Assert.AreEqual(0, page.BlockId);
            Assert.AreEqual(100, page.PageSize);
            Assert.AreEqual(PageStatus.New, page.PageStatus);
        }

        [Test]
        public void CanCreatePageFromExistingByteArray()
        {
            var data = new byte[100];
            data[0] = 1;
            data[1] = 2;
            data[99] = 3;

            var page = new Page(0, data);

            Assert.IsNotNull(page);
            Assert.AreEqual(0, page.BlockId);
            Assert.AreEqual(100, page.PageSize);
            Assert.AreEqual(PageStatus.Commited, page.PageStatus);
            Assert.AreEqual(data.Length, page.Data.Length);
            Assert.AreEqual(data[0], page.Data[0]);
            Assert.AreEqual(data[1], page.Data[1]);
            Assert.AreEqual(data[2], page.Data[2]);
            Assert.AreEqual(data[99], page.Data[99]);
        }

        [Test]
        public void CanWriteBool()
        {
            var page = new Page(0, 100);
            page.SetBool(0, true);

            Assert.GreaterOrEqual(page.Data[0], 1);
        }

        [Test]
        public void CanReadBool()
        {
            var page = new Page(0, 100);
            page.SetBool(0, true);
            var result = page.GetBool(0, out var value);

            Assert.IsTrue(result);
            Assert.AreEqual(true, value);
        }

        [Test]
        public void CanWriteByte()
        {
            var page = new Page(0, 100);
            page.SetByte(0, 123);

            Assert.AreEqual(123, page.Data[0]);
        }

        [Test]
        public void CanReadByte()
        {
            var page = new Page(0, 100);
            page.SetByte(0, 123);
            var result = page.GetByte(0, out var value);

            Assert.IsTrue(result);
            Assert.AreEqual(123, value);
        }

        [Test]
        public void CanWriteInt()
        {
            var page = new Page(0, 100);
            page.SetInt(0, 123);

            Assert.AreEqual(123, page.Data[0]);
        }

        [Test]
        public void CanReadInt()
        {
            var page = new Page(0, 100);
            page.SetInt(0, 123);
            var result = page.GetInt(0, out var value);

            Assert.IsTrue(result);
            Assert.AreEqual(123, value);
        }

        [Test]
        public void CanWriteBlob()
        {
            var page = new Page(0, 100);
            var blob = new byte[] { 1, 2, 3 };
            page.SetBlob(0, blob);

            Assert.AreEqual(3, page.Data[0]);
            Assert.AreEqual(1, page.Data[sizeof(uint)]);
            Assert.AreEqual(2, page.Data[sizeof(uint) + 1]);
            Assert.AreEqual(3, page.Data[sizeof(uint) + 2]);
        }

        [Test]
        public void CanReadBlob()
        {
            var page = new Page(0, 100);
            var blob = new byte[] { 1, 2, 3 };
            page.SetBlob(0, blob);
            var result = page.GetBlob(0, out var value);

            Assert.IsTrue(result);
            Assert.IsNotNull(value);
            Assert.AreEqual(3, value.Length);
            Assert.AreEqual(1, value[0]);
            Assert.AreEqual(2, value[1]);
            Assert.AreEqual(3, value[2]);
        }

        [Test]
        public void CanReadAndWriteString()
        {
            var page = new Page(0, 100);
            var str = "greeting";
            page.SetString(0, str);
            var result = page.GetString(0, out var value);

            Assert.IsTrue(result);
            Assert.IsNotNull(value);
            Assert.AreEqual(str, value);
        }

        [Test]
        public void CanReadAndWriteDateTime()
        {
            var page = new Page(0, 100);
            var datetime = new DateTime(2020, 1, 1);
            page.SetDate(0, datetime);
            var result = page.GetDate(0, out var value);

            Assert.IsTrue(result);
            Assert.IsNotNull(value);
            Assert.AreEqual(datetime, value);
        }

        [Test]
        public void SequentialIntReadAndWrite()
        {
            var page = new Page(0, 100);
            var int1 = 123;
            var int2 = 456;

            page.Position = 0;
            page.SetInt(int1);
            page.SetInt(int2);

            page.Position = 0;

            var int1Result = page.GetInt(out var int1ResultValue);
            var int2Result = page.GetInt(out var int2ResultValue);

            Assert.IsTrue(int1Result);
            Assert.IsTrue(int2Result);
            Assert.AreEqual(int1, int1ResultValue);
            Assert.AreEqual(int2, int2ResultValue);
        }

        [Test]
        public void SequentialByteReadAndWrite()
        {
            var page = new Page(0, 100);
            var var1 = (byte)123;
            var var2 = (byte)211;

            page.Position = 0;
            page.SetByte(var1);
            page.SetByte(var2);

            page.Position = 0;

            var var1Result = page.GetByte(out var var1ResultValue);
            var var2Result = page.GetByte(out var var2ResultValue);

            Assert.IsTrue(var1Result);
            Assert.IsTrue(var2Result);
            Assert.AreEqual(var1, var1ResultValue);
            Assert.AreEqual(var2, var2ResultValue);
        }

        [Test]
        public void SequentialBoolReadAndWrite()
        {
            var page = new Page(0, 100);
            var var1 = true;
            var var2 = false;

            page.Position = 0;
            page.SetBool(var1);
            page.SetBool(var2);

            page.Position = 0;

            var var1Result = page.GetBool(out var var1ResultValue);
            var var2Result = page.GetBool(out var var2ResultValue);

            Assert.IsTrue(var1Result);
            Assert.IsTrue(var2Result);
            Assert.AreEqual(var1, var1ResultValue);
            Assert.AreEqual(var2, var2ResultValue);
        }

        [Test]
        public void SequentialBlobReadAndWrite()
        {
            var page = new Page(0, 100);
            var var1 = new byte[] { 1, 2, 3 };
            var var2 = new byte[] { 4, 5, 6 };

            page.Position = 0;
            page.SetBlob(var1);
            page.SetBlob(var2);

            page.Position = 0;

            var var1Result = page.GetBlob(out var var1ResultValue);
            var var2Result = page.GetBlob(out var var2ResultValue);

            Assert.IsTrue(var1Result);
            Assert.IsTrue(var2Result);
            Assert.AreEqual(var1, var1ResultValue);
            Assert.AreEqual(var2, var2ResultValue);
        }

        [Test]
        public void SequentialStringReadAndWrite()
        {
            var page = new Page(0, 100);
            var var1 = "hello";
            var var2 = "world";

            page.Position = 0;
            page.SetString(var1);
            page.SetString(var2);

            page.Position = 0;

            var var1Result = page.GetString(out var var1ResultValue);
            var var2Result = page.GetString(out var var2ResultValue);

            Assert.IsTrue(var1Result);
            Assert.IsTrue(var2Result);
            Assert.AreEqual(var1, var1ResultValue);
            Assert.AreEqual(var2, var2ResultValue);
        }

        [Test]
        public void SequentialDateTimeReadAndWrite()
        {
            var page = new Page(0, 100);
            var var1 = new DateTime(2020, 1, 1);
            var var2 = new DateTime(2000, 3, 3);

            page.Position = 0;
            page.SetDate(var1);
            page.SetDate(var2);

            page.Position = 0;

            var var1Result = page.GetDate(out var var1ResultValue);
            var var2Result = page.GetDate(out var var2ResultValue);

            Assert.IsTrue(var1Result);
            Assert.IsTrue(var2Result);
            Assert.AreEqual(var1, var1ResultValue);
            Assert.AreEqual(var2, var2ResultValue);
        }

        [Test]
        public void SequentialReadsAndWrites()
        {
            var page = new Page(0, 1000);
            var intValue = 123;
            var byteValue = (byte)99;
            var boolValue = true;
            var blobValue = new byte[] { 1, 2, 3 };
            var stringValue = "temp string";
            var dateValue = new DateTime(2020, 1, 1);

            // Not nessesary, just to make it clear where we starts
            page.Position = 0;

            page.SetInt(intValue);
            page.SetByte(byteValue);
            page.SetBool(boolValue);
            page.SetBlob(blobValue);
            page.SetString(stringValue);
            page.SetDate(dateValue);

            page.Position = 0;
            var intResult = page.GetInt(out var resultIntValue);
            var byteResult = page.GetByte(out var resultByteValue);
            var boolResult = page.GetBool(out var resultBoolValue);
            var blobResult = page.GetBlob(out var resultBlobValue);
            var stringResult = page.GetString(out var resultStringValue);
            var dateResult = page.GetDate(out var resultDateValue);

            Assert.IsTrue(intResult);
            Assert.AreEqual(intValue, resultIntValue);
            Assert.IsTrue(byteResult);
            Assert.AreEqual(byteValue, resultByteValue);
            Assert.IsTrue(boolResult);
            Assert.AreEqual(boolValue, resultBoolValue);
            Assert.IsTrue(byteResult);
            Assert.AreEqual(byteValue, resultByteValue);
            Assert.IsTrue(blobResult);
            Assert.AreEqual(blobValue, resultBlobValue);
            Assert.IsTrue(stringResult);
            Assert.AreEqual(stringValue, resultStringValue);
            Assert.IsTrue(dateResult);
            Assert.AreEqual(dateValue, resultDateValue);
        }
    }
}