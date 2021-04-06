using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Record.Base
{
    public class TableInfo
    {
        private readonly string _filename;
        private readonly Schema _schema;


        public TableInfo(string tableName, Schema schema)
        {

        }

        public TableInfo(string tableName, Schema schema, Dictionary<string, int> offsets, int recordLength)
        {

        }

        public int Offset(string fieldName)
        {
            return default;
        }

        public int RecordLength()
        {
            return default;
        }

        public string Filename => _filename;
        public Schema Schema => _schema;
    }
}
