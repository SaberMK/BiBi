using BB.Metadata.Abstract;
using BB.Record.Base;
using BB.Transactions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Metadata.Indexes.Implementations
{
    public class BTreeIndex : BaseIndex
    {
        public BTreeIndex(string indexName, Schema schema, Transaction transaction)
        {
            // TODO
        }

        public static int SearchCost(int blocksCount, int recordsPerBlock)
        {
            // TODO
            return 0;
        }
    }
}
