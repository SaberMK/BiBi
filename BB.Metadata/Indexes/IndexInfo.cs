using BB.Metadata.Abstract;
using BB.Metadata.Indexes.Implementations;
using BB.Metadata.Statistic;
using BB.Metadata.Table;
using BB.Record.Base;
using BB.Transactions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Metadata.Indexes
{
    public class IndexInfo
    {
        private readonly string _indexName;
        private readonly string _fieldName;

        // Would become in handy later when index types would be > 1 
        private readonly int _indexType;

        private readonly Transaction _transaction;
        private readonly TableInfo _tableInfo;
        private readonly StatisticalInfo _statInfo;

        private readonly int _blockSize;

        public IndexInfo(string indexName, 
            string tableName, 
            string fieldName, 
            ITableManager tableManager, 
            IStatisticsManager statisticsManager, 
            Transaction transaction, 
            int blockSize = 1024)
        {
            _indexName = indexName;
            _fieldName = fieldName;
            _transaction = transaction;

            _blockSize = blockSize;
            _tableInfo = tableManager.GetTableInfo(tableName, transaction);
            _statInfo = statisticsManager.GetStatisticalInfo(tableName, transaction);
        }

        public BaseIndex Open()
        {
            var schema = CreateSchema();
            return new BTreeIndex(_indexName, schema, _transaction);
        }

        public int BlocksAccessed()
        {
            var recordsPerBlock = _blockSize / _tableInfo.RecordLength;
            int blocksCount = _statInfo.RecordsOutput / recordsPerBlock;

            return BTreeIndex.SearchCost(blocksCount, recordsPerBlock);
        }

        public int RecordsOutput()
        {
            return _statInfo.RecordsOutput / _statInfo.DistinctValues(_fieldName);
        }

        private Schema CreateSchema()
        {
            var schema = new Schema();
            schema.AddIntField("blk");
            schema.AddIntField("id");

            // TODO for all types
            var indexType = _tableInfo.Schema.FieldType(_fieldName);
            switch (indexType)
            {
                case FieldType.Bool:
                    // lol dude why u want BTree on bool??? Are you insane?

                    schema.AddBoolField("val");
                    break;


                case FieldType.Byte:

                    schema.AddByteField("val");
                    break;


                case FieldType.Integer:

                    schema.AddIntField("val");
                    break;


                case FieldType.Date:

                    schema.AddDateField("val");
                    break;


                case FieldType.Blob:

                    var blobFieldLength = _tableInfo.Schema.Length(_fieldName);
                    schema.AddBlobField("val", blobFieldLength);
                    break;


                case FieldType.String:

                    var stringFieldLength = _tableInfo.Schema.Length(_fieldName);
                    schema.AddStringField("val", stringFieldLength);
                    break;
            }

            return schema;
        }
    }
}
