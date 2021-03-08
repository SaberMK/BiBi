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
    }
}