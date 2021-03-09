using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using System;
using System.Collections.Generic;
using System.Text;

namespace BB.Memory
{
    public class LogManager : ILogManager
    {
        private readonly IFileManager _fileManager;
        private Page _logPage;
        private int _blockId;

        private int _latestLSN;
        private int _latestSavedLSN;
        private int _boundary;

        internal int LatestLSN => _latestLSN;
        internal int LatestSavedLSN => _latestSavedLSN;
        internal int Boundary => _boundary;

        public LogManager(IFileManager fileManager)
        {
            _fileManager = fileManager;
            _blockId = _fileManager.LastBlockId;
            
            if(_fileManager.Length == 0)
            {
                _logPage = _fileManager.Append();
                _boundary = 0;
            }
            else
            {
                _ = _fileManager.Read(_fileManager.LastBlockId, out _logPage);
                _ = _logPage.GetInt(0,out _boundary);
            }

        }

        public bool Append(byte[] data, out int lsn)
        {
            var totalSize = data.Length + sizeof(int);
            if (totalSize > _logPage.PageSize)
            {
                lsn = 0;
                return false;
            }

            if (_boundary + totalSize > _logPage.PageSize)
            {
                Flush(_latestLSN);

                _logPage = _fileManager.Append();
                _boundary = 0;
            }

            _boundary = _boundary + totalSize;
            _ = _logPage.SetBlob(_boundary, data);
            _ = _logPage.SetInt(0, _boundary);

            _latestLSN++;
            lsn = _latestLSN;

            return true;
        }

        public IEnumerator<byte[]> Enumerator()
        {
            throw new NotImplementedException();
        }

        public void Flush(int lsn)
        {
            if (lsn > _latestSavedLSN)
                Flush();
        }

        public void Dispose()
        {
            _fileManager?.Dispose();
        }

        private void Flush()
        {
            _ = _fileManager.Write(_logPage);
            _latestSavedLSN = _latestLSN;
        }
    }
}
