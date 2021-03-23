using BB.IO.Primitives;
using NUnit.Framework;

namespace BB.IO.Tests
{
    public class BlockTests
    {
        [Test]
        public void CanCreateBlock()
        {
            var block = new Block("tmp", 0);

            Assert.AreEqual("tmp", block.Filename);
            Assert.AreEqual(0, block.Id);
        }

        [Test]
        public void HashcodeNotRandomByStringAndId()
        {
            var block = new Block("tmp", 0);
            int hashcode;

            unchecked
            {
                hashcode = block.Filename.GetHashCode() ^ 0;
            }

            Assert.AreEqual(hashcode, block.GetHashCode());
        }

        [Test]
        public void ToStringReturnsMeaningfullInformation()
        {
            var block = new Block("tmp", 0);
            var str = block.ToString();

            Assert.IsTrue(str.Contains(block.Filename));
            Assert.IsTrue(str.Contains(block.Id.ToString()));
        }

        [Test]
        public void TwoSameBlocksAreEqual()
        {
            var block1 = new Block("tmp1", 0);
            var block2 = new Block("tmp1", 0);

            var equal = block1.Equals(block2);
            Assert.IsTrue(equal);
        }

        [Test]
        public void BlockAndPackedBlockAreEqual()
        {
            var block1 = new Block("tmp1", 0);
            var block2 = new Block("tmp1", 0);

            var equal = block1.Equals((object)block2);
            Assert.IsTrue(equal);
        }

        [Test]
        public void CannotCompareBlockWithNull()
        {
            var block1 = new Block("tmp1", 0);

            var equal = block1.Equals((Block?)null);
            Assert.IsFalse(equal);
        }

        [Test]
        public void EqualAndNotEqualOperatorsAreWorking()
        {
            var block1 = new Block("tmp1", 0);
            var block2 = new Block("tmp1", 0);
            var block3 = new Block("tmp2", 1);

            var equal1 = block1 == block2;
            var equal2 = block2 == block1;
            var notEqual = block1 != block3;

            Assert.IsTrue(equal1);
            Assert.IsTrue(equal2);
            Assert.IsTrue(notEqual);
        }
    }
}
