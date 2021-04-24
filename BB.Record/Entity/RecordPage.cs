using BB.IO.Abstract;
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
    public class RecordPage
    {
        private readonly IFileManager _fileManager;

        public const byte EMPTY = 0;
        public const byte INUSE = 1;

        private Block? _block;
        private TableInfo _tableInfo;
        private Transaction _transaction;
        private int _slotSize;
        private int _currentSlot = -1;

        public RecordPage(Block block, TableInfo tableInfo, Transaction transaction, IFileManager fileManager, int currentSlot = -1)
        {
            _block = block;
            _tableInfo = tableInfo;
            _transaction = transaction;
            _fileManager = fileManager;

            _transaction.Pin(_block.Value);
            _slotSize = _tableInfo.RecordLength + sizeof(int);

            _currentSlot = currentSlot;
        }

        public void Close()
        {
            if (_block != null)
                _transaction.Unpin(_block.Value);

            _block = null;
        }

        public bool Next()
        {
            return SearchFor(INUSE);
        }

        #region Set Methods

        public void SetInt(string fieldName, int value)
        {
            var position = FieldPosition(fieldName);
            _transaction.SetInt(_block.Value, position, value);
        }

        public void SetBool(string fieldName, bool value)
        {
            var position = FieldPosition(fieldName);
            _transaction.SetBool(_block.Value, position, value);
        }

        public void SetByte(string fieldName, byte value)
        {
            var position = FieldPosition(fieldName);
            _transaction.SetByte(_block.Value, position, value);
        }

        public void SetBlob(string fieldName, byte[] value)
        {
            var position = FieldPosition(fieldName);
            _transaction.SetBlob(_block.Value, position, value);
        }

        public void SetString(string fieldName, string value)
        {
            var position = FieldPosition(fieldName);
            _transaction.SetString(_block.Value, position, value);
        }

        public void SetDate(string fieldName, DateTime value)
        {
            var position = FieldPosition(fieldName);
            _transaction.SetDate(_block.Value, position, value);
        }

        #endregion

        #region Get Methods

        public int GetInt(string fieldName)
        {
            var position = FieldPosition(fieldName);
            _ = _transaction.GetInt(_block.Value, position, out var value);
            return value;
        }

        public bool GetBool(string fieldName)
        {
            var position = FieldPosition(fieldName);
            _ = _transaction.GetBool(_block.Value, position, out var value);
            return value;
        }

        public byte GetByte(string fieldName)
        {
            var position = FieldPosition(fieldName);
            _ = _transaction.GetByte(_block.Value, position, out var value);
            return value;
        }

        public byte[] GetBlob(string fieldName)
        {
            var position = FieldPosition(fieldName);
            _ = _transaction.GetBlob(_block.Value, position, out var value);
            return value;
        }

        public string GetString(string fieldName)
        {
            var position = FieldPosition(fieldName);
            _ = _transaction.GetString(_block.Value, position, out var value);
            return value;
        }

        public DateTime GetDate(string fieldName)
        {
            var position = FieldPosition(fieldName);
            _ = _transaction.GetDate(_block.Value, position, out var value);
            return value;
        }

        #endregion

        public void Delete()
        {
            var position = CurrentPosition();
            _transaction.SetInt(_block.Value, position, EMPTY);
        }

        public bool Insert()
        {
            _currentSlot = -1;
            var found = SearchFor(EMPTY);
            if (found)
            {
                var position = CurrentPosition();
                _transaction.SetInt(_block.Value, position, INUSE);
            }

            return found;
        }

        public void MoveToId(int id)
        {
            _currentSlot = id;
        }

        public int CurrentId => _currentSlot;

        private int CurrentPosition()
        {
            return _currentSlot * _slotSize;
        }

        private int FieldPosition(string fieldName)
        {
            int offset = sizeof(int) + _tableInfo.Offset(fieldName);
            return CurrentPosition() + offset;
        }

        private bool IsValidSlot()
        {
            return CurrentPosition() + _slotSize < _fileManager.BlockSize;
        }

        private bool SearchFor(int flag)
        {
            _currentSlot++;
            while(IsValidSlot())
            {
                int position = CurrentPosition();
                _ = _transaction.GetInt(_block.Value, position, out var value);

                if (value == flag)
                    return true;

                _currentSlot++;
            }

            return false;
        }
    }
}
