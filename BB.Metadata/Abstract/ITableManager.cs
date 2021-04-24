using BB.Record.Base;
using BB.Transactions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Metadata.Abstract
{
    public interface ITableManager
    {
        void CreateTable(string tableName, Schema schema, Transaction transaction);
        TableInfo GetTableInfo(string tableName, Transaction transaction);
    }
}
