using BB.Metadata.Abstract;
using BB.Record.Base;
using BB.Record.Entity;
using BB.Transactions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Metadata.Indexes
{
    public class IndexManager : IIndexManager
    {
        // TODO should divide it for separate name, table and field 
        private const int MAX_INDEX_LENGTH = 0;

        private readonly TableInfo _tableInfo;
        private readonly string _indexTableName;
        private readonly IMetadataManager _metadataManager;
        public IndexManager(
            bool isNew, 
            ITableManager tableManager, 
            Transaction transaction, 
            IMetadataManager metadataManager, 
            string indexTableName = "idxcat")
        {
            _indexTableName = indexTableName;
            _metadataManager = metadataManager;

            if (isNew)
            {
                var schema = new Schema();

                schema.AddStringField("idxname", MAX_INDEX_LENGTH);
                schema.AddStringField("tblname", MAX_INDEX_LENGTH);
                schema.AddStringField("fldname", MAX_INDEX_LENGTH);

                tableManager.CreateTable(_indexTableName, schema, transaction);
            }

            _tableInfo = tableManager.GetTableInfo(indexTableName, transaction);
        }

        public void CreateIndex(string indexName, string tableName, string fieldName, Transaction transaction)
        {
            var recordFile = new RecordFile(_tableInfo, transaction);

            recordFile.Insert();
            recordFile.SetString("idxname", indexName);
            recordFile.SetString("tblname", tableName);
            recordFile.SetString("fldname", fieldName);
            recordFile.Close();
        }

        public Dictionary<string, IndexInfo> GetIndexInfo(string tableName, Transaction transaction)
        {
            var result = new Dictionary<string, IndexInfo>();
            var recordFile = new RecordFile(_tableInfo, transaction);

            while (recordFile.Next())
            {
                if (recordFile.GetString("tblname") == tableName)
                {
                    var indexName = recordFile.GetString("idxname");
                    var fieldName = recordFile.GetString("fldname");

                    var indexInfo = new IndexInfo(indexName, tableName, fieldName, _metadataManager, transaction);

                    result.Add(fieldName, indexInfo);
                }
            }

            recordFile.Close();

            return result;
        }
    }
}
