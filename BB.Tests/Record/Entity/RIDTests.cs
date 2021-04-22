using BB.Record.Entity;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Tests.Record.Entity
{
    [TestFixture]
    public class RIDTests
    {
        [Test]
        public void CanCreateRID()
        {
            RID rid = new RID(1, 2); ;

            Assert.AreEqual(1, rid.BlockNumber);
            Assert.AreEqual(2, rid.Id);
        }
    }
}
