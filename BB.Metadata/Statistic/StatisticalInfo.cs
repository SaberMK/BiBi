using BB.Metadata.Abstract;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Metadata.Statistic
{
    public class StatisticalInfo
    {
        private int _blocksCount;
        private int _recordsCount;

        public StatisticalInfo(int blocksCount, int recordsCount)
            => (_blocksCount, _recordsCount) = (blocksCount, recordsCount);

        public int BlocksAccessed => _blocksCount;

        public int RecordsOutput => _recordsCount;

        // Lol hacky solution
        public int DistinctValues(string fieldName)
            => 1 + (_recordsCount / 3);

    }
}
