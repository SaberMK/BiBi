using BB.Metadata.Statistic;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Tests.Metadata.Statistic
{
    [TestFixture]
    public class StatisticalInfoTests
    {
        StatisticalInfo info;

        [SetUp]
        public void Setup()
        {

        }

        [Test]
        public void CanCreateStatisticalInfo()
        {
            Assert.DoesNotThrow(() =>
            {
                info = new StatisticalInfo(10, 10);
            });
        }

        [Test]
        public void CanGetBlocksAccessed()
        {
            info = new StatisticalInfo(10, 10);
            var result = info.BlocksAccessed;

            Assert.AreEqual(10, result);
        }

        [Test]
        public void CanGetRecordsOutput()
        {
            info = new StatisticalInfo(10, 10);
            var result = info.RecordsOutput;

            Assert.AreEqual(10, result);
        }

        [Test]
        public void CanGetDistinctValuesCount()
        {
            info = new StatisticalInfo(6, 6);
            var result = info.DistinctValues("file");

            Assert.AreEqual(3, result);
        }
    }
}
