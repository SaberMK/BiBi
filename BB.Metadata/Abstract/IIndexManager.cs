using BB.Metadata.Indexes;
using BB.Transactions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Metadata.Abstract
{
    public interface IIndexManager
    {
        void CreateIndex(string indexName, string tableName, string fieldName, Transaction transaction);
        Dictionary<string, IndexInfo> GetIndexInfo(string tableName, Transaction transaction);
    }
}
