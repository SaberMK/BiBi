using BB.Query.Abstract;
using BB.Query.Expressions;
using BB.Record.Base;
using BB.Record.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Scans
{
    public class TableScan : IUpdateScan
    {
        private RecordFile _recordFile;
        private Schema _schema;

        public RID RID => _recordFile.CurrentRID;

        public TableScan(
            RecordFile recordFile,
            Schema schema)
        {
            _recordFile = recordFile;
            _schema = schema;
        }

        public void BeforeFirst()
            => _recordFile.BeforeFirst();

        public bool Next()
            => _recordFile.Next();

        public void Close()
            => _recordFile.Close();

        public Constant GetValue(string fieldName)
        {
            var type = _schema.FieldType(fieldName);

            switch(type)
            {
                case FieldType.Bool:
                    return new Constant<bool>(_recordFile.GetBool(fieldName));

                case FieldType.Byte:
                    return new Constant<byte>(_recordFile.GetByte(fieldName));

                case FieldType.Integer:
                    return new Constant<int>(_recordFile.GetInt(fieldName));

                case FieldType.Blob:
                    return new Constant<byte[]>(_recordFile.GetBlob(fieldName));

                case FieldType.Date:
                    return new Constant<DateTime>(_recordFile.GetDate(fieldName));
            }

            return null;
        }

        public int GetInt(string fieldName)
            => _recordFile.GetInt(fieldName);

        public bool GetBool(string fieldName)
            => _recordFile.GetBool(fieldName);

        public byte GetByte(string fieldName)
            => _recordFile.GetByte(fieldName);

        public byte[] GetBlob(string fieldName)
            => _recordFile.GetBlob(fieldName);

        public string GetString(string fieldName)
            => _recordFile.GetString(fieldName);

        public DateTime GetDate(string fieldName)
            => _recordFile.GetDate(fieldName);

        public void SetValue(string fieldName, Constant value)
        {
            var type = _schema.FieldType(fieldName);

            switch (type)
            {
                case FieldType.Bool:
                    _recordFile.SetBool(fieldName, (value as Constant<bool>).Value);
                    return;

                case FieldType.Byte:
                    _recordFile.SetByte(fieldName, (value as Constant<byte>).Value);
                    return;

                case FieldType.Integer:
                    _recordFile.SetInt(fieldName, (value as Constant<int>).Value);
                    return;

                case FieldType.Blob:
                    _recordFile.SetBlob(fieldName, (value as Constant<byte[]>).Value);
                    return;

                case FieldType.Date:
                    _recordFile.SetDate(fieldName, (value as Constant<DateTime>).Value);
                    return;
            }
        }

        public void SetInt(string fieldName, int value)
            => _recordFile.SetInt(fieldName, value);

        public void SetByte(string fieldName, byte value)
            => _recordFile.SetByte(fieldName, value);

        public void SetBool(string fieldName, bool value)
            => _recordFile.SetBool(fieldName, value);

        public void SetBlob(string fieldName, byte[] value)
            => _recordFile.SetBlob(fieldName, value);

        public void SetString(string fieldName, string value)
            => _recordFile.SetString(fieldName, value);

        public void SetDate(string fieldName, DateTime value)
            => _recordFile.SetDate(fieldName, value);

        public bool HasField(string fieldName)
            => _schema.HasField(fieldName);

        public void MoveToRID(RID rid)
            => _recordFile.MoveToRID(rid);
    }
}
