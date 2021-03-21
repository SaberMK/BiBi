using BB.IO.Primitives;
using System;
using System.Collections.Generic;
using System.Text;

namespace BB.Memory.Base
{
    // TODO think about ref BasicLogRecord!!!
    public struct BasicLogRecord
    {
        private readonly Page _page;
        private int _position;

        public BasicLogRecord(Page page, int position)
        {
            _page = page;
            _position = position;
        }

        public bool NextInt(out int value)
        {
            if (!_page.GetInt(_position, out value))
                return false;

            _position += sizeof(int);
            return true;
        }

        public bool NextByte(out byte value)
        {
            if (!_page.GetByte(_position, out value))
                return false;

            _position += sizeof(byte);
            return true;
        }

        public bool NextBool(out bool value)
        {
            if (!_page.GetBool(_position, out value))
                return false;

            _position += sizeof(bool);
            return true;
        }

        public bool NextBlob(out byte[] value)
        {
            if (!_page.GetBlob(_position, out value))
                return false;

            _position += value.Length;
            return true;
        }

        public bool NextString(out string value)
        {
            if (!_page.GetString(_position, out value))
                return false;

            // Maybe it would cause troubles with different encodings
            _position += value.Length;
            return true;
        }

        public bool NextDate(out DateTime value)
        {
            if (!_page.GetDate(_position, out value))
                return false;

            _position += sizeof(long);
            return true;
        }
    }
}
