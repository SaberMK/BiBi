using BB.Query.Abstract;
using BB.Query.Expressions;
using BB.Record.Entity;
using System;

namespace BB.Query.Scans
{
    public class SelectScan : IUpdateScan
    {
        private readonly IScan _scan;
        private readonly Predicate _predicate;

        public SelectScan(IScan scan, Predicate predicate)
        {
            _scan = scan;
            _predicate = predicate;
        }

        public void BeforeFirst() => _scan.BeforeFirst();

        public bool Next()
        {
            while (_scan.Next())
            {
                if (_predicate.IsSatisfied(_scan))
                    return true;
            }

            return false;
        }

        public void Close() => _scan.Close();
        public bool HasField(string fieldName) => _scan.HasField(fieldName);

        public Constant GetValue(string fieldName) => _scan.GetValue(fieldName);
        public int GetInt(string fieldName) => _scan.GetInt(fieldName);
        public byte GetByte(string fieldName) => _scan.GetByte(fieldName);
        public bool GetBool(string fieldName) => _scan.GetBool(fieldName);
        public string GetString(string fieldName) => _scan.GetString(fieldName);
        public byte[] GetBlob(string fieldName) => _scan.GetBlob(fieldName);
        public DateTime GetDate(string fieldName) => _scan.GetDate(fieldName);

        public void SetValue(string fieldName, Constant value)
        {
            var updateScan = _scan as IUpdateScan;
            if(updateScan != null)
            {
                updateScan.SetValue(fieldName, value);
            }
        }

        public void SetInt(string fieldName, int value)
        {
            var updateScan = _scan as IUpdateScan;
            if (updateScan != null)
            {
                updateScan.SetInt(fieldName, value);
            }
        }

        public void SetByte(string fieldName, byte value)
        {
            var updateScan = _scan as IUpdateScan;
            if (updateScan != null)
            {
                updateScan.SetByte(fieldName, value);
            }
        }

        public void SetBool(string fieldName, bool value)
        {
            var updateScan = _scan as IUpdateScan;
            if (updateScan != null)
            {
                updateScan.SetBool(fieldName, value);
            }
        }

        public void SetBlob(string fieldName, byte[] value)
        {
            var updateScan = _scan as IUpdateScan;
            if (updateScan != null)
            {
                updateScan.SetBlob(fieldName, value);
            }
        }

        public void SetString(string fieldName, string value)
        {
            var updateScan = _scan as IUpdateScan;
            if (updateScan != null)
            {
                updateScan.SetString(fieldName, value);
            }
        }

        public void SetDate(string fieldName, DateTime value)
        {
            var updateScan = _scan as IUpdateScan;
            if (updateScan != null)
            {
                updateScan.SetDate(fieldName, value);
            }
        }

        public RID RID
        {
            get
            {
                var updateScan = _scan as IUpdateScan;
                if (updateScan != null)
                {
                    return updateScan.RID;
                }
                return RID.MissedRID;
            }
        }

        public void MoveToRID(RID rid)
        {
            var updateScan = _scan as IUpdateScan;
            if (updateScan != null)
            {
                updateScan.MoveToRID(rid);
            }
        }

        public void Insert()
        {
            var updateScan = _scan as IUpdateScan;
            if (updateScan != null)
            {
                updateScan.Insert();
            }
        }

        public void Delete()
        {
            var updateScan = _scan as IUpdateScan;
            if (updateScan != null)
            {
                updateScan.Delete();
            }
        }
    }
}
