using BB.Record.Base;
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
        void CreateTable(string tableName, Schema schema, ITransaction transaction);
        TableInfo GetTableInfo(string tableName, ITransaction transaction);

        void CreateView(string viewName, string viewDefinition, ITransaction transaction);
        string GetViewDefinition(string viewName, ITransaction transaction);

        void CreateIndex(string indexName, string tableName, string fieldName, ITransaction transaction);
        Dictionary<string, TableInfo> GetIndexInfo(string tableName, ITransaction transaction);

        TableInfo GetStatisticsInfo(string tableName, TableInfo tableInfo, ITransaction transaction);
    }
}
