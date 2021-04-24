using BB.Metadata.Abstract;
using BB.Record.Base;
using BB.Record.Entity;
using BB.Transactions;
using BB.Transactions.Abstract;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Metadata.Table
{
    public class TableManager : ITableManager
    {
        public const int MAX_NAME_LENGTH = 50;

        private readonly TableInfo _tableCatalogInfo;
        private readonly TableInfo _fieldCatalogInfo;

        private readonly string _tableCatalogName;
        private readonly string _fieldCatalogName;

        public TableManager(
            bool isNew, 
            Transaction transaction, 
            string tableCatalogName = "tblname", 
            string fieldCatalogName = "fldcat")
        {
            _tableCatalogName = tableCatalogName;
            _fieldCatalogName = fieldCatalogName;

            var tableCatalogSchema = new Schema();
            tableCatalogSchema.AddStringField("tblname", MAX_NAME_LENGTH);
            tableCatalogSchema.AddIntField("reclength");

            _tableCatalogInfo = new TableInfo(_tableCatalogName, tableCatalogSchema);

            var fieldCatalogSchema = new Schema();
            fieldCatalogSchema.AddStringField("tblname", MAX_NAME_LENGTH);
            fieldCatalogSchema.AddStringField("fldname", MAX_NAME_LENGTH);
            fieldCatalogSchema.AddIntField("type");
            fieldCatalogSchema.AddIntField("length");
            fieldCatalogSchema.AddIntField("offset");

            _fieldCatalogInfo = new TableInfo(_fieldCatalogName, fieldCatalogSchema);

            if(isNew)
            {
                CreateTable(_tableCatalogName, tableCatalogSchema, transaction);
                CreateTable(_fieldCatalogName, fieldCatalogSchema, transaction);
            }
        }

        public void CreateTable(string tableName, Schema schema, Transaction transaction)
        {
            var tableInfo = new TableInfo(tableName, schema);

            var tableCatalogFile = new RecordFile(_tableCatalogInfo, transaction);
            tableCatalogFile.Insert();
            tableCatalogFile.SetString("tblname", tableName);
            tableCatalogFile.SetInt("reclength", tableInfo.RecordLength);
            tableCatalogFile.Close();

            var fieldCatalogFile = new RecordFile(_fieldCatalogInfo, transaction);
            foreach (var fieldName in schema.Fields)
            {
                fieldCatalogFile.Insert();
                fieldCatalogFile.SetString("tblname", tableName);
                fieldCatalogFile.SetString("fldname", fieldName.Key);
                fieldCatalogFile.SetInt("type", (int)fieldName.Value.Type);
                fieldCatalogFile.SetInt("length", fieldName.Value.Length);
                fieldCatalogFile.SetInt("offset", tableInfo.Offset(fieldName.Key));
            }
            fieldCatalogFile.Close();
        }

        public TableInfo GetTableInfo(string tableName, Transaction transaction)
        {
            var tableCatalogFile = new RecordFile(_tableCatalogInfo, transaction);
            var recordLength = -1;

            while (tableCatalogFile.Next())
            {
                if(tableCatalogFile.GetString("tblname") == tableName)
                {
                    recordLength = tableCatalogFile.GetInt("reclength");
                    break;
                }
            }
            tableCatalogFile.Close();

            if(recordLength == -1)
            {
                return null;
            }

            var fieldCatalogFile = new RecordFile(_fieldCatalogInfo, transaction);
            var schema = new Schema();
            var offsets = new Dictionary<string, int>();
            while (fieldCatalogFile.Next())
            {
                if(fieldCatalogFile.GetString("tblname") == tableName)
                {
                    var fieldName = fieldCatalogFile.GetString("fldname");
                    int fieldType = fieldCatalogFile.GetInt("type");
                    int fieldLength = fieldCatalogFile.GetInt("length");
                    int offset = fieldCatalogFile.GetInt("offset");
                    offsets.Add(fieldName, offset);
                    schema.AddField(fieldName, (FieldType)fieldType, fieldLength);
                }
            }
            fieldCatalogFile.Close();

            return new TableInfo(tableName, schema, offsets, recordLength);
        }
    }
}
