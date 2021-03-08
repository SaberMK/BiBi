using BB.IO.Primitives;
using NUnit.Framework;

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
            Assert.IsNotNull(page.Data);
            Assert.AreEqual(new byte[100], page.Data);
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
            Assert.IsNotNull(page.Data);
            Assert.AreEqual(data.Length, page.Data.Length);
            Assert.AreEqual(data[0], page.Data[0]);
            Assert.AreEqual(data[1], page.Data[1]);
            Assert.AreEqual(data[2], page.Data[2]);
            Assert.AreEqual(data[99], page.Data[99]);
        }
    }
}