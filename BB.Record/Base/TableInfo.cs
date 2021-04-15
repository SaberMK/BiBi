using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Record.Base
{
    public class TableInfo
    {
        private readonly string _tableName;
        private readonly Schema _schema;
        private int _recordLength;
        private readonly Dictionary<string, int> _offsets;

        public TableInfo(string tableName, Schema schema)
        {
            _schema = schema;
            _tableName = tableName;
            _offsets = new Dictionary<string, int>();

            int position = 0;
            foreach (var field in schema.Fields)
            {
                _offsets.Add(field.Key, position);
                position += LengthInBytes(field.Key);
            }

            _recordLength += position;
        }

        public TableInfo(string tableName, Schema schema, Dictionary<string, int> offsets, int recordLength)
        {
            _tableName = tableName;
            _schema = schema;
            _offsets = offsets;
            _recordLength = recordLength;
        }

        public int Offset(string fieldName)
        {
            return _offsets.TryGetValue(fieldName, out var data) ? data : -1;
        }

        private int LengthInBytes(string fieldName)
        {
            var fieldType = _schema.FieldType(fieldName);

            switch(fieldType)
            {
                case FieldType.Bool or FieldType.Byte:
                    return sizeof(bool);

                case FieldType.Integer:
                    return sizeof(int);

                case FieldType.Blob or FieldType.String:
                    return sizeof(int) + _schema.Length(fieldName);

                case FieldType.Date:
                    return sizeof(long);
            }

            return -1;
        }

        public int RecordLength => _recordLength;
        public string Filename => _tableName + ".tbl";
        public Schema Schema => _schema;
    }
}
