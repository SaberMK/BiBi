using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Record.Base
{
    public class Schema
    {
        private readonly Dictionary<string, FieldInfo> _fields;

        public Schema()
        {
            _fields = new Dictionary<string, FieldInfo>();
        }


        public void AddField(string fieldName, FieldType type, int length)
        {
            _fields.Add(fieldName, new FieldInfo(type, length));
        }

        public void AddIntField(string fieldName)
        {
            AddField(fieldName, Base.FieldType.Integer, sizeof(int));
        }

        public void AddStringField(string fieldName, int length)
        {
            AddField(fieldName, Base.FieldType.Integer, sizeof(int)+length);
        }

        public void Add(string fieldName, Schema schema)
        {
            var type = schema.FieldType(fieldName);
            var length = schema.Length(fieldName);
            AddField(fieldName, type, length);
        }

        public void AddAll(Schema schema)
        {
            var fields = schema.Fields;

            foreach(var entry in fields)
            {
                if (!_fields.ContainsKey(entry.Key))
                    _fields.Add(entry.Key, entry.Value);
            }
        }

        public bool HasField(string fieldName)
        {
            return _fields.ContainsKey(fieldName);
        }

        public FieldType FieldType(string fieldName)
        {
            return _fields[fieldName].Type;
        }

        public int Length(string fieldName)
        {
            return _fields[fieldName].Length;
        }

        public Dictionary<string, FieldInfo> Fields => _fields;
    }
}
