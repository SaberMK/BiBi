using BB.IO.Abstract;
using BB.Metadata.Abstract;
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

namespace BB.Metadata
{
    public class MetadataManager : IMetadataManager
    {
        private readonly ITableManager _tableManager;
        private readonly IViewManager _viewManager;
        private readonly IIndexManager _indexManager;
        private readonly IStatisticsManager _statisticsManager;
        private readonly int _blockSize;

        public MetadataManager(
            IFileManager fileManager,
            ITableManager tableManager,
            IViewManager viewManager,
            IIndexManager indexManager,
            IStatisticsManager statisticsManager)
        {
            _tableManager = tableManager;
            _viewManager = viewManager;
            _indexManager = indexManager;
            _statisticsManager = statisticsManager;
            _blockSize = fileManager.BlockSize;
        }

        public int BlockSize => _blockSize;

        public void CreateIndex(string indexName, string tableName, string fieldName, Transaction transaction)
        {
            _indexManager.CreateIndex(indexName, tableName, fieldName, transaction);
        }

        public Dictionary<string, IndexInfo> GetIndexInfo(string tableName, Transaction transaction)
        {
            return _indexManager.GetIndexInfo(tableName, transaction);
        }

        public void CreateTable(string tableName, Schema schema, Transaction transaction)
        {
            _tableManager.CreateTable(tableName, schema, transaction);
        }

        public TableInfo GetTableInfo(string tableName, Transaction transaction)
        {
            return _tableManager.GetTableInfo(tableName, transaction);
        }

        public void CreateView(string viewName, string viewDefinition, Transaction transaction)
        {
            _viewManager.CreateView(viewName, viewDefinition, transaction);
        }

        public string GetViewDefinition(string viewName, Transaction transaction)
        {
            return _viewManager.GetViewDefinition(viewName, transaction);
        }

        public StatisticalInfo GetStatisticsInfo(string tableName, Transaction transaction)
        {
            return _statisticsManager.GetStatisticalInfo(tableName, transaction);
        }
    }
}
