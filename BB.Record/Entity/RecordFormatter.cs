using BB.IO.Abstract;
using BB.IO.Primitives;
using BB.Memory.Abstract;
using BB.Record.Base;
using BB.Record.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Record.Entity
{
    public class RecordFormatter : IPageFormatter
    {
        private readonly TableInfo _tableInfo;
        private readonly IFileManager _fileManager;
        // TODO Rethink about all the classes!!! TIRED OF INJECTING EVERYTHING IN CONSTRUCTOR!!!
        public RecordFormatter(TableInfo tableInfo, IFileManager fileManager)
        {
            _tableInfo = tableInfo;
            _fileManager = fileManager;
        }

        public void Format(Page page)
        {
            int recordSize = _tableInfo.RecordLength + sizeof(int);
            for(var position = 0;position + recordSize <= _fileManager.BlockSize; position += recordSize)
            {
                page.SetInt(position, RecordPage.EMPTY);

                // TODO this all can be compled as an expression, I think

                MakeDefaultRecord(page, position);
            }
        }

        private void MakeDefaultRecord(Page page, int position)
        {
            foreach (var field in _tableInfo.Schema.Fields)
            {
                int offset = _tableInfo.Offset(field.Key);
                
                switch(_tableInfo.Schema.FieldType(field.Key))
                {
                    // TODO I think that int every time is not needed

                    case FieldType.Bool:
                        page.SetBool(position + sizeof(int) + offset, false);
                        break;

                    case FieldType.Byte:
                        page.SetByte(position + sizeof(int) + offset, 0);
                        break;

                    case FieldType.Integer:
                        page.SetInt(position + sizeof(int) + offset, 0);
                        break;

                    case FieldType.Blob:
                        page.SetBlob(position + sizeof(int) + offset, new byte[] { });
                        break;

                    case FieldType.String:
                        page.SetString(position + sizeof(int) + offset, "");
                        break;

                    case FieldType.Date:
                        page.SetDate(position + sizeof(int) + offset, new DateTime());
                        break;
                }

            }
        }
            
    }
}
