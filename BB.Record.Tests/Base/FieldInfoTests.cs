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
    public class FieldInfoTests
    {
        [Test]
        public void CanCreateFieldInfo()
        {
            var fi = new FieldInfo(FieldType.Blob, 10);

            Assert.IsNotNull(fi);
            Assert.AreEqual(FieldType.Blob, fi.Type);
            Assert.AreEqual(10, fi.Length);
        }
    }
}
