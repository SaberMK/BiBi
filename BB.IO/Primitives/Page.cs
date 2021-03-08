using System;
using System.Collections.Generic;
using System.Text;

namespace BB.IO.Primitives
{
    // TODO think about it: should all read/writes be dirty/free for race conditions?
    public struct Page
    {
        private readonly int _blockId;
        private readonly int _pageSize;

        // It needs to be internal for FileStream.Write(...)
        internal readonly byte[] _data;
        internal PageStatus _status;

        internal ReadOnlySpan<byte> Data => _data;
        public int BlockId => _blockId;
        public int PageSize => _pageSize;
        public PageStatus PageStatus => _status;

        // I think would add multiple lock objects in future
        public object LockObject { get; private set; }

        public static readonly Encoding Encoding = Encoding.ASCII;

        public Page(int blockId, int pageSize)
        {
            _blockId = blockId;
            _data = new byte[pageSize];
            _pageSize = _data.Length;
            _status = PageStatus.New;
            LockObject = new object();
        }

        internal Page(int blockId, byte[] data)
        {
            _blockId = blockId;
            _data = data;
            _pageSize = _data.Length;
            _status = PageStatus.Commited;
            LockObject = new object();
        }

        public bool SetInt(int offset, int value)
        {
            if(offset < 0 
                || offset + sizeof(int) > _pageSize)
            {
                return false;
            }

            var convertedValue = BitConverter.GetBytes(value);
            Array.Copy(convertedValue, 0, _data, offset, 4);
            return true;
        }

        public bool GetInt(int offset, out int value)
        {
            if(offset < 0 || offset > _pageSize)
            {
                value = default;
                return false;
            }

            value = BitConverter.ToInt32(_data, offset);
            return true;
        }

        public bool SetBool(int offset, bool value)
        {
            if (offset < 0
                || offset + sizeof(bool) > _pageSize)
            {
                return false;
            }


            _data[offset] = (byte)(value ? 1 : 0);
            return true;
        }

        public bool GetBool(int offset, out bool value)
        {
            if (offset < 0 || offset > _pageSize)
            {
                value = default;
                return false;
            }

            value = _data[offset] > 0;
            return true;
        }
        public bool SetByte(int offset, byte value)
        {
            if (offset < 0
                || offset + sizeof(byte) > _pageSize)
            {
                return false;
            }


            _data[offset] = value;
            return true;
        }

        public bool GetByte(int offset, out byte value)
        {
            if (offset < 0 || offset > _pageSize)
            {
                value = default;
                return false;
            }

            value = _data[offset];
            return true;
        }

        public bool GetBlob(int offset, out byte[] value)
        {
            if (offset < 0 || offset > _pageSize)
            {
                value = default;
                return false;
            }

            var length = BitConverter.ToUInt32(_data, offset);

            value = new byte[length];
            Array.Copy(_data, offset + sizeof(uint), value, 0, length);
            return true;
        }

        public bool SetBlob(int offset, byte[] blob)
        {
            if(offset < 0 || offset + blob.Length + sizeof(uint) > _pageSize)
            {
                return false;
            }

            var blobLength = BitConverter.GetBytes((uint)blob.Length);
            Array.Copy(blobLength, 0, _data, offset, blobLength.Length);

            Array.Copy(blob, 0, _data, offset + sizeof(uint), blob.Length);
            return true;
        }

        public bool GetString(int offset, out string value)
        {
            if (offset < 0 || offset > _pageSize)
            {
                value = default;
                return false;
            }

            var length = BitConverter.ToUInt32(_data, offset);

            value = Encoding.GetString(_data, offset + sizeof(uint), (int)length);
            return true;
        }

        public bool SetString(int offset, string value)
        {
            if(offset < 0 
                || offset + value.Length + sizeof(uint) > _pageSize)
            {
                return false;
            }

            var blobLength = BitConverter.GetBytes((uint)value.Length);
            Array.Copy(blobLength, 0, _data, offset, blobLength.Length);

            Array.Copy(Encoding.GetBytes(value), 0, _data, offset + sizeof(uint), value.Length);
            return true;
        }

        public bool SetDate(int offset, DateTime value)
        {
            // sizeof(long) because DateTime.ToBinary is long
            if (offset < 0
                || offset + sizeof(long) > _pageSize)
            {
                return false;
            }

            var bytes = BitConverter.GetBytes(value.ToBinary());
            Array.Copy(bytes, 0, _data, offset, sizeof(long));
            return true;
        }

        public bool GetDate(int offset, out DateTime value)
        {
            if (offset < 0 || offset > _pageSize)
            {
                value = default;
                return false;
            }

            var bytes = new byte[sizeof(long)];
            Array.Copy(_data, offset, bytes, 0, sizeof(long));
            value = DateTime.FromBinary(BitConverter.ToInt64(bytes));
            return true;
        }
    }
}
