using BB.Query.Abstract;
using BB.Query.Expressions;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace BB.Query.Scans
{
    public class ProjectScan : IScan
    {
        private readonly IScan _scan;
        private readonly ICollection<string> _fieldList;

        public ProjectScan(IScan scan, ICollection<string> fieldList)
        {
            _scan = scan;
            _fieldList = fieldList;
        }

        public void BeforeFirst() => _scan.BeforeFirst();
        public bool Next() => _scan.Next();
        public void Close() => _scan.Close();

        // I'm a fucking idiot. I should behave this bool CanGetSomthing(string fieldName, out Something value)
        // Fucking retard.

        public Constant GetValue(string fieldName)
        {
            if(HasField(fieldName))
            {
                return _scan.GetValue(fieldName);
            }
            return default;
        }

        public int GetInt(string fieldName)
        {
            if (HasField(fieldName))
            {
                return _scan.GetInt(fieldName);
            }
            return default;
        }

        public byte GetByte(string fieldName)
        {
            if (HasField(fieldName))
            {
                return _scan.GetByte(fieldName);
            }
            return default;
        }

        public bool GetBool(string fieldName)
        {
            if (HasField(fieldName))
            {
                return _scan.GetBool(fieldName);
            }
            return default;
        }

        public byte[] GetBlob(string fieldName)
        {
            if (HasField(fieldName))
            {
                return _scan.GetBlob(fieldName);
            }
            return default;
        }

        public string GetString(string fieldName)
        {
            if (HasField(fieldName))
            {
                return _scan.GetString(fieldName);
            }
            return default;
        }

        public DateTime GetDate(string fieldName)
        {
            if (HasField(fieldName))
            {
                return _scan.GetDate(fieldName);
            }
            return default;
        }

        // TODO think maybe make it with aggressive inlining?
        public bool HasField(string fieldName) => _fieldList.Contains(fieldName);

    }
}
