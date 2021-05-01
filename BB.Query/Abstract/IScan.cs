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
        void Next();
        void Close();
        bool HasField(string fieldName);


        Constant GetValue(string fieldName);
        int GetInt(string fieldName);
        int GetBool(string fieldName);
        int GetByte(string fieldName);
        int GetBlob(string fieldName);
        int GetString(string fieldName);
        int GetDate(string fieldName);
    }
}
