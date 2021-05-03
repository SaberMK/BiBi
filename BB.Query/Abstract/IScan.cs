using BB.Query.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Abstract
{
    public interface IScan
    {
        void BeforeFirst();
        bool Next();
        void Close();
        bool HasField(string fieldName);

        Constant GetValue(string fieldName);
        int GetInt(string fieldName);
        bool GetBool(string fieldName);
        byte GetByte(string fieldName);
        byte[] GetBlob(string fieldName);
        string GetString(string fieldName);
        DateTime GetDate(string fieldName);
    }
}
