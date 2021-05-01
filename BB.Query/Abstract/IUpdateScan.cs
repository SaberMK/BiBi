using BB.Record.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Abstract
{
    public interface IUpdateScan : IScan
    {
        void SetValue(string fieldName, Constant constant);
        void SetInt(string fieldName);
        void SetBool(string fieldName);
        void SetByte(string fieldName);
        void SetBlob(string fieldName);
        void SetString(string fieldName);
        void SetDate(string fieldName);

        RID GetRID { get; }
        void MoveToRID(RID rid);
    }
}
