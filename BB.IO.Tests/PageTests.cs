using BB.IO.Abstract;
using BB.IO.Primitives;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BB.IO.Tests
{
    public class PageTests
    {
        private IFileManager _fileManager;

        [SetUp]
        public void Setup()
        {
            _fileManager = new FileManager("temp", "DBs", 100);
        }

        [Test]
        public void CanCreatePageWithSize()
        {
            var filename = RandomFilename;
            var page = _fileManager.ResolvePage(new Block(filename, 0));

            Assert.IsNotNull(page);
            Assert.AreEqual(0, page.Block.Id);
            Assert.AreEqual(filename, page.Block.Filename);
            Assert.AreEqual(100, page.PageSize);
        }

        [Test]
        public void CanCreatePageFromExistingByteArray()
        {
            var filename = RandomFilename;

            var data = new byte[100];
            data[0] = 1;
            data[1] = 2;
            data[99] = 3;

            var page = _fileManager.ResolvePage(new Block(filename, 0), data);

            Assert.IsNotNull(page);
            Assert.AreEqual(0, page.Block.Id);
            Assert.AreEqual(filename, page.Block.Filename);
            Assert.AreEqual(100, page.PageSize);
            Assert.AreEqual(data.Length, page.Data.Length);
            Assert.AreEqual(data[0], page.Data[0]);
            Assert.AreEqual(data[1], page.Data[1]);
            Assert.AreEqual(data[2], page.Data[2]);
            Assert.AreEqual(data[99], page.Data[99]);
        }

        [Test]
        public void CanWriteBool()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));
            page.SetBool(0, true);
            page.SetBool(1, false);

            Assert.GreaterOrEqual(page.Data[0], 1);
            Assert.AreEqual(0, page.Data[1]);
        }

        [Test]
        public void CanReadBool()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));
            page.SetBool(0, true);
            var result = page.GetBool(0, out var value);

            Assert.IsTrue(result);
            Assert.True(value);
        }

        [Test]
        public void CanWriteByte()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));
            page.SetByte(0, 123);

            Assert.AreEqual(123, page.Data[0]);
        }

        [Test]
        public void CanReadByte()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));
            page.SetByte(0, 123);
            var result = page.GetByte(0, out var value);

            Assert.IsTrue(result);
            Assert.AreEqual(123, value);
        }

        [Test]
        public void CanWriteInt()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));
            page.SetInt(0, 123);

            Assert.AreEqual(123, page.Data[0]);
        }

        [Test]
        public void CanReadInt()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));
            page.SetInt(0, 123);
            var result = page.GetInt(0, out var value);

            Assert.IsTrue(result);
            Assert.AreEqual(123, value);
        }

        [Test]
        public void CanWriteBlob()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));
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
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));
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
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));
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
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));
            var datetime = new DateTime(2020, 1, 1);
            page.SetDate(0, datetime);
            var result = page.GetDate(0, out var value);

            Assert.IsTrue(result);
            Assert.IsNotNull(value);
            Assert.AreEqual(datetime, value);
        }

        [Test]
        public void CannotSetIntIfOutOfRange()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));

            Assert.DoesNotThrow(() =>
            {
                var canSet1 = page.SetInt(-1, 1);
                var canSet2 = page.SetInt(page.PageSize + 1, 1);

                Assert.False(canSet1);
                Assert.False(canSet2);
            });
        }

        [Test]
        public void CannotGetIntIfOutOfRange()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));

            Assert.DoesNotThrow(() =>
            {
                var canSet1 = page.GetInt(-1, out var value1);
                var canSet2 = page.GetInt(page.PageSize + 1, out var value2);

                Assert.False(canSet1);
                Assert.False(canSet2);
                Assert.AreEqual(default(int), value1);
                Assert.AreEqual(default(int), value2);
            });
        }

        [Test]
        public void CannotSetStringIfOutOfRange()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));

            Assert.DoesNotThrow(() =>
            {
                var canSet1 = page.SetString(-1, "abc");
                var canSet2 = page.SetString(page.PageSize + 1, "abc");

                Assert.False(canSet1);
                Assert.False(canSet2);
            });
        }

        [Test]
        public void CannotGetStringIfOutOfRange()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));

            Assert.DoesNotThrow(() =>
            {
                var canSet1 = page.GetString(-1, out var value1);
                var canSet2 = page.GetString(page.PageSize + 1, out var value2);

                Assert.False(canSet1);
                Assert.False(canSet2);
                Assert.AreEqual(default(string), value1); 
                Assert.AreEqual(default(string), value2);
            });
        }

        [Test]
        public void CannotSetDateIfOutOfRange()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));

            Assert.DoesNotThrow(() =>
            {
                var canSet1 = page.SetDate(-1, new DateTime(2020, 1, 1));
                var canSet2 = page.SetDate(page.PageSize + 1, new DateTime(2020, 1, 1));

                Assert.False(canSet1);
                Assert.False(canSet2);
            });
        }

        [Test]
        public void CannotGetDateIfOutOfRange()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));

            Assert.DoesNotThrow(() =>
            {
                var canSet1 = page.GetDate(-1, out var value1);
                var canSet2 = page.GetDate(page.PageSize + 1, out var value2);

                Assert.False(canSet1);
                Assert.False(canSet2);
                Assert.AreEqual(default(DateTime), value1);
                Assert.AreEqual(default(DateTime), value2);
            });
        }

        [Test]
        public void CannotSetByteIfOutOfRange()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));

            Assert.DoesNotThrow(() =>
            {
                var canSet1 = page.SetByte(-1, 1);
                var canSet2 = page.SetByte(page.PageSize + 1, 1);

                Assert.False(canSet1);
                Assert.False(canSet2);
            });
        }

        [Test]
        public void CannotGetByteIfOutOfRange()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));

            Assert.DoesNotThrow(() =>
            {
                var canSet1 = page.GetByte(-1, out var value1);
                var canSet2 = page.GetByte(page.PageSize + 1, out var value2);

                Assert.False(canSet1);
                Assert.False(canSet2);
                Assert.AreEqual(default(byte), value1);
                Assert.AreEqual(default(byte), value2);
            });
        }

        [Test]
        public void CannotSetBoolIfOutOfRange()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));

            Assert.DoesNotThrow(() =>
            {
                var canSet1 = page.SetBool(-1, true);
                var canSet2 = page.SetBool(page.PageSize + 1, true);

                Assert.False(canSet1);
                Assert.False(canSet2);
            });
        }

        [Test]
        public void CannotGetBoolIfOutOfRange()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));

            Assert.DoesNotThrow(() =>
            {
                var canSet1 = page.GetBool(-1, out var value1);
                var canSet2 = page.GetBool(page.PageSize + 1, out var value2);

                Assert.False(canSet1);
                Assert.False(canSet2);
                Assert.AreEqual(default(bool), value1);
                Assert.AreEqual(default(bool), value2);
            });
        }

        [Test]
        public void CannotSetBlobIfOutOfRange()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));

            Assert.DoesNotThrow(() =>
            {
                var canSet1 = page.SetBlob(-1, new byte[] { 1, 2 });
                var canSet2 = page.SetBlob(page.PageSize + 1, new byte[] { 1, 2 });

                Assert.False(canSet1);
                Assert.False(canSet2);
            });
        }

        [Test]
        public void CannotGetBlobIfOutOfRange()
        {
            var page = _fileManager.ResolvePage(new Block(RandomFilename, 0));

            Assert.DoesNotThrow(() =>
            {
                var canSet1 = page.GetBlob(-1, out var value1);
                var canSet2 = page.GetBlob(page.PageSize + 1, out var value2);

                Assert.False(canSet1);
                Assert.False(canSet2);
                Assert.AreEqual(default(byte[]), value1);
                Assert.AreEqual(default(byte[]), value2);
            });
        }

        [Test]
        public void CanCreateEmptyPage()
        {
            var page = _fileManager.ResolvePage();

            Assert.IsNotNull(page);
            Assert.AreEqual(_fileManager.BlockSize, page.PageSize);
        }

        [Test]
        public void CanWritePageToDisk()
        {
            var filename = RandomFilename;
            var block = new Block(filename, 0);
            var page = _fileManager.ResolvePage(block);

            page.SetInt(0, 123);
            page.Write(block);

            var pageBuffer = page.Data;
            var canReadPage = _fileManager.Read(block, out var buffer);

            Assert.True(canReadPage);
            for(var i = 0; i < buffer.Length; ++i)
            {
                Assert.AreEqual(pageBuffer[i], buffer[i]);
            }
        }

        [Test]
        public void CanReadPageFromDisk()
        {
            var filename = RandomFilename;
            var block = new Block(filename, 0);
            var page = _fileManager.ResolvePage(block);

            page.SetInt(0, 123);
            page.Write(block);

            var pageBuffer = page.Data;
            var canReadAsPage = page.Read(block);

            Assert.True(canReadAsPage);
            for (var i = 0; i < page.Data.Length; ++i)
            {
                Assert.AreEqual(pageBuffer[i], page.Data[i]);
            }
        }

        [Test]
        public void CanAppendPage()
        {
            var filename = RandomFilename;
            var block1 = new Block(filename, 0);
            var page = _fileManager.ResolvePage(block1);
            page.Write(block1);
            var canAppend = page.Append(filename, out var block2);

            Assert.True(canAppend);
            Assert.AreEqual(1, block2.Id);
            Assert.AreEqual(filename, block2.Filename);
        }

        private string RandomFilename => $"{Guid.NewGuid()}.bin";
    }
}
