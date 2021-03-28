using BB.Memory.Base;
using DateTime = System.DateTime;

namespace BB.Transactions.Abstract
{
    public interface IRecoveryManager
    {
        void Commit();
        void Rollback();
        void Recover();

        int SetInt(Buffer buffer, int offset, int value);
        int SetByte(Buffer buffer, int offset, byte value);
        int SetBool(Buffer buffer, int offset, bool value);
        int SetBlob(Buffer buffer, int offset, byte[] value);
        int SetString(Buffer buffer, int offset, string value);
        int SetDate(Buffer buffer, int offset, DateTime value);
    }
}
