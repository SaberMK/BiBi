using BB.Query.Abstract;
using BB.Query.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Scans
{
    public class ProductScan : IScan
    {
        private readonly IScan _scan1;
        private readonly IScan _scan2;

        public ProductScan(IScan scan1, IScan scan2)
        {
            _scan1 = scan1;
            _scan2 = scan2;
        }

        public void BeforeFirst()
        {
            _scan1.BeforeFirst();
            _scan1.Next();
            _scan2.BeforeFirst();
        }

        public bool Next()
        {
            if (_scan2.Next())
                return true;

            _scan2.BeforeFirst();
            return _scan2.Next() && _scan1.Next();
        }

        public void Close()
        {
            _scan1.Close();
            _scan2.Close();
        }

        // TODO I'm a fucking idiot. Would not change to bool CanGetSomething(string fieldName, out Something value)
        // as a valuable lesson to myself. Would not make this mistake in future

        public Constant GetValue(string fieldName)
        {
            if (_scan1.HasField(fieldName))
                return _scan1.GetValue(fieldName);

            if (_scan2.HasField(fieldName))
                return _scan2.GetValue(fieldName);

            return default;
        }

        public int GetInt(string fieldName)
        {
            if (_scan1.HasField(fieldName))
                return _scan1.GetInt(fieldName);

            if (_scan2.HasField(fieldName))
                return _scan2.GetInt(fieldName);

            return default;
        }

        public byte GetByte(string fieldName)
        {
            if (_scan1.HasField(fieldName))
                return _scan1.GetByte(fieldName);

            if (_scan2.HasField(fieldName))
                return _scan2.GetByte(fieldName);

            return default;
        }

        public bool GetBool(string fieldName)
        {
            if (_scan1.HasField(fieldName))
                return _scan1.GetBool(fieldName);

            if (_scan2.HasField(fieldName))
                return _scan2.GetBool(fieldName);

            return default;
        }


        public byte[] GetBlob(string fieldName)
        {
            if (_scan1.HasField(fieldName))
                return _scan1.GetBlob(fieldName);

            if (_scan2.HasField(fieldName))
                return _scan2.GetBlob(fieldName);

            return default;
        }

        public string GetString(string fieldName)
        {
            if (_scan1.HasField(fieldName))
                return _scan1.GetString(fieldName);

            if (_scan2.HasField(fieldName))
                return _scan2.GetString(fieldName);

            return default;
        }

        public DateTime GetDate(string fieldName)
        {
            if (_scan1.HasField(fieldName))
                return _scan1.GetDate(fieldName);

            if (_scan2.HasField(fieldName))
                return _scan2.GetDate(fieldName);

            return default;
        }

        public bool HasField(string fieldName)
            => _scan1.HasField(fieldName) || _scan2.HasField(fieldName);
    }
}
