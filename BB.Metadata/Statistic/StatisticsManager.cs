using BB.Metadata.Abstract;
using BB.Record.Base;
using BB.Record.Entity;
using BB.Transactions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Metadata.Statistic
{
    public class StatisticsManager
    {
        private readonly Dictionary<string, StatisticalInfo> _tableStats;

        private readonly object _lockObject = new object();
        private readonly int _statisticCallsBeforeUpdateCount;
        private readonly IMetadataManager _metadataManager;

        private int _callsNumber;

        public StatisticsManager(IMetadataManager metadataManager, Transaction transaction, int statisticCallsBeforeUpdateCount = 100)
        {
            _statisticCallsBeforeUpdateCount = statisticCallsBeforeUpdateCount;
            _tableStats = new Dictionary<string, StatisticalInfo>();

            RefreshStatistics(transaction);
        }

        public StatisticalInfo GetStatisticalInfo(string tableName, Transaction transaction)
        {
            lock (_lockObject)
            {
                _callsNumber++;

                if(_callsNumber > _statisticCallsBeforeUpdateCount)
                {
                    RefreshStatistics(transaction);
                }

                var hasValue = _tableStats.TryGetValue(tableName, out var stat);
                
                if(!hasValue)
                {
                    stat = RefreshTableStatistics(tableName, transaction);
                }

                return stat;
            }
        }

        private void RefreshStatistics(Transaction transaction)
        {
            _tableStats.Clear();

            _callsNumber = 0;

            var tableCatalogInfo = _metadataManager.GetTableInfo("tblcat", transaction);
            var recordFile = new RecordFile(tableCatalogInfo, transaction);

            while (recordFile.Next())
            {
                var tableName = recordFile.GetString("tblname");
                _ = RefreshTableStatistics(tableName, transaction);
            }
        }

        private StatisticalInfo RefreshTableStatistics(string tableName, Transaction transaction)
        {
            var recordsCount = 0;

            var tableInfo = _metadataManager.GetTableInfo(tableName, transaction);
            var recordFile = new RecordFile(tableInfo, transaction);

            while (recordFile.Next())
            {
                recordsCount++;
            }

            int blockNumber = recordFile.CurrentRID.BlockNumber;
            recordFile.Close();

            int blocksCount = 1 + blockNumber;
            var statInfo = new StatisticalInfo(blocksCount, recordsCount);

            _tableStats.Add(tableName, statInfo);
            return statInfo;
        }
    }
}
