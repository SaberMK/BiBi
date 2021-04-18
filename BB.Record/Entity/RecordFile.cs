using BB.IO.Primitives;
using BB.Record.Base;
using BB.Transactions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Record.Entity
{
    public class RecordFile
    {
        private readonly TableInfo _tableInfo;
        private readonly Transaction _transaction;
        private readonly string _filename;
        private int _currentBlockNumber;
        private RecordPage _recordPage;

        public RecordFile(TableInfo tableInfo, Transaction transaction)
        {
            _tableInfo = tableInfo;
            _transaction = transaction;

            _filename = tableInfo.Filename;

            if (transaction.Length(_filename) == 0)
                AppendBlock();

            MoveTo(0);
        }

        public void Close()
        {
            _recordPage.Close();
        }

        private void BeforeFirst()
        {
            MoveTo(0);
        }

        private Boolean Next()
        {
            while(true)
            {
                if (_recordPage.Next())
                    return true;
                if (AtLastBlock)
                    return false;
                MoveTo(_currentBlockNumber + 1);
            }
        }

        public int GetInt(string fieldName)
        {
            return _recordPage.GetInt(fieldName);
        }

        public byte GetByte(string fieldName)
        {
            return _recordPage.GetByte(fieldName);
        }

        public bool GetBool(string fieldName)
        {
            return _recordPage.GetBool(fieldName);
        }

        public byte[] GetBlob(string fieldName)
        {
            return _recordPage.GetBlob(fieldName);
        }

        public string GetString(string fieldName)
        {
            return _recordPage.GetString(fieldName);
        }

        public DateTime GetDate(string fieldName)
        {
            return _recordPage.GetDate(fieldName);
        }

        public void SetInt(string fieldName, int value)
        {
            _recordPage.SetInt(fieldName, value);
        }

        public void SetByte(string fieldName, byte value)
        {
            _recordPage.SetByte(fieldName, value);
        }

        public void SetBool(string fieldName,bool value)
        {
            _recordPage.SetBool(fieldName, value);
        }

        public void SetBlob(string fieldName, byte[] value)
        {
            _recordPage.SetBlob(fieldName, value);
        }

        public void SetString(string fieldName, string value)
        {
            _recordPage.SetString(fieldName, value);
        }

        public void SetDate(string fieldName, DateTime value)
        {
            _recordPage.SetDate(fieldName, value);
        }

        public void Delete()
        {
            _recordPage.Delete();
        }

        public void Insert()
        {
            while(!_recordPage.Insert())
            {
                if(AtLastBlock)
                {
                    AppendBlock();
                }
                MoveTo(_currentBlockNumber + 1);
            }
        }

        public void MoveToRID(RID rid)
        {
            MoveTo(rid.BlockNumber);
            _recordPage.MoveToId(rid.Id);
        }

        public RID CurrentRID => new RID(_currentBlockNumber, _recordPage.CurrentId);

        private void MoveTo(int blockId)
        {
            if (_recordPage != null)
                _recordPage.Close();

            _currentBlockNumber = blockId;
            Block block = new Block(_filename, _currentBlockNumber);
            _recordPage = new RecordPage(block, _tableInfo, _transaction, _transaction.FileManager);
        }

        private bool AtLastBlock => _currentBlockNumber == _transaction.Length(_filename) - 1;

        private void AppendBlock()
        {
            var recordFormatter = new RecordFormatter(_tableInfo, _transaction.FileManager);
            _transaction.Append(_filename, recordFormatter);
        }
    }
}
