using BB.Metadata.Abstract;
using BB.Metadata.Statistic;
using BB.Query.Abstract;
using BB.Query.Scans;
using BB.Record.Base;
using BB.Transactions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Plans
{
    public class TablePlan : IPlan
    {
        private readonly Transaction _transaction;
        private readonly TableInfo _tableInfo;
        private readonly StatisticalInfo _statisticalInfo;

        public TablePlan(string tableName, IMetadataManager metadataManager, Transaction transaction)
        {
            _transaction = transaction;
            _tableInfo = metadataManager.GetTableInfo(tableName, transaction);
            _statisticalInfo = metadataManager.GetStatisticsInfo(tableName, transaction);
        }

        public IScan Open() => new TableScan(_tableInfo, _transaction);

        public int BlocksAccessed => _statisticalInfo.BlocksAccessed;

        public int RecordsOutput => _statisticalInfo.RecordsOutput;

        public Schema Schema => _tableInfo.Schema;

        public int DistinctValues(string fieldName) => _statisticalInfo.DistinctValues(fieldName);
    }
}
