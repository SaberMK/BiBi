using BB.Memory.Base;
using DateTime = System.DateTime;

namespace BB.Transactions.Abstract
{
    public interface IRecoveryManager
    {
        void Commit();
        void Rollback();
        void Recover();

        bool SetInt(Buffer buffer, int offset, int value);
        bool SetByte(Buffer buffer, int offset, byte value);
        bool SetBool(Buffer buffer, int offset, bool value);
        bool SetBlob(Buffer buffer, int offset, byte[] value);
        bool SetString(Buffer buffer, int offset, string value);
        bool SetDate(Buffer buffer, int offset, DateTime value);
    }
}
