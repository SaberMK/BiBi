using BB.Query.Expressions;
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
        void SetInt(string fieldName, int value);
        void SetBool(string fieldName, bool value);
        void SetByte(string fieldName, byte value);
        void SetBlob(string fieldName, byte[] value);
        void SetString(string fieldName, string value);
        void SetDate(string fieldName, DateTime value);

        RID RID { get; }
        void MoveToRID(RID rid);
    }
}
