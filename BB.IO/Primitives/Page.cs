using BB.IO.Abstract;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace BB.IO.Primitives
{
    public class Page
    {
        private readonly IFileManager _fileManager;
        private readonly int _pageSize;
        private static readonly Encoding Encoding = Encoding.ASCII;

        private Block _block;
        private byte[] _data;
        private int _position;
        internal ReadOnlySpan<byte> Data => _data;

        public int PageSize => _pageSize;
        public Block Block => _block;

        public Page(IFileManager fileManager, int pageSize)
        {
            _fileManager = fileManager;
            _position = 0;
            _block = default;
            _pageSize = pageSize;
            _data = new byte[pageSize];
        }

        public Page(IFileManager fileManager, Block block, int pageSize)
        {
            _fileManager = fileManager;
            _position = 0;
            _block = block;
            _pageSize = pageSize;
            _data = new byte[pageSize];
        }

        internal Page(IFileManager fileManager)
        {
            _fileManager = fileManager;
            _position = 0;
            _block = default;
            _pageSize = fileManager.BlockSize;
            _data = new byte[_pageSize];
        }

        internal Page(IFileManager fileManager, Block block, byte[] data)
        {
            _fileManager = fileManager;
            _position = 0;
            _block = block;
            _pageSize = data.Length;
            _data = data;
        }

        public bool Read(Block block)
        {
            _block = block;
            return _fileManager.Read(block, out _data);
        }

        public bool Write(Block block)
        {
            return _fileManager.Write(block, _data);
        }

        public bool Append(string filename, out Block block)
        {
            return _fileManager.Append(filename, out block);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SetInt(int offset, int value)
        {
            if (offset < 0
                || offset + sizeof(int) > _pageSize)
            {
                return false;
            }

            var convertedValue = BitConverter.GetBytes(value);
            Array.Copy(convertedValue, 0, _data, offset, 4);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetInt(int offset, out int value)
        {
            if (offset < 0 || offset > _pageSize)
            {
                value = default;
                return false;
            }

            value = BitConverter.ToInt32(_data, offset);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SetBlob(int offset, byte[] blob)
        {
            if (offset < 0 || offset + blob.Length + sizeof(uint) > _pageSize)
            {
                return false;
            }

            var blobLength = BitConverter.GetBytes((uint)blob.Length);
            Array.Copy(blobLength, 0, _data, offset, blobLength.Length);

            Array.Copy(blob, 0, _data, offset + sizeof(uint), blob.Length);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetBlob(int offset, out byte[] value)
        {
            if (offset < 0 || offset > _pageSize)
            {
                value = default;
                return false;
            }

            var length = BitConverter.ToInt32(_data, offset);

            value = new byte[length];
            Array.Copy(_data, offset + sizeof(uint), value, 0, length);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SetString(int offset, string value)
        {
            if (offset < 0
                || offset + value.Length + sizeof(uint) > _pageSize)
            {
                return false;
            }

            var blobLength = BitConverter.GetBytes((uint)value.Length);
            Array.Copy(blobLength, 0, _data, offset, blobLength.Length);

            Array.Copy(Encoding.GetBytes(value), 0, _data, offset + sizeof(uint), value.Length);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
