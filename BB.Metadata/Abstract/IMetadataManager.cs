using BB.Metadata.Indexes;
using BB.Metadata.Statistic;
using BB.Record.Base;
using BB.Transactions;
using BB.Transactions.Abstract;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Metadata.Abstract
{
    public interface IMetadataManager
    {
        void CreateTable(string tableName, Schema schema, Transaction transaction);
        TableInfo GetTableInfo(string tableName, Transaction transaction);

        void CreateView(string viewName, string viewDefinition, Transaction transaction);
        string GetViewDefinition(string viewName, Transaction transaction);

        void CreateIndex(string indexName, string tableName, string fieldName, Transaction transaction);
        Dictionary<string, IndexInfo> GetIndexInfo(string tableName, Transaction transaction);

        StatisticalInfo GetStatisticsInfo(string tableName, Transaction transaction);

        int BlockSize { get; }
    }
}
