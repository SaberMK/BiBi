using BB.Metadata.Abstract;
using BB.Metadata.Table;
using BB.Record.Base;
using BB.Record.Entity;
using BB.Transactions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Metadata.View
{
    public class ViewManager : IViewManager
    {
        // SHOULD BE NVARCHAR(MAX) !!!
        public const int MAX_VIEW_LENGTH = 100;

        private readonly ITableManager _tableManager;
        private readonly string _viewTableName;
        public ViewManager(bool isNew, ITableManager tableManager, Transaction transaction, string viewTableName = "viewcat")
        {
            _tableManager = tableManager;
            _viewTableName = viewTableName;

            if (isNew)
            {
                var schema = new Schema();
                schema.AddStringField("viewname", MAX_VIEW_LENGTH);
                schema.AddStringField("viewdef", MAX_VIEW_LENGTH);

                tableManager.CreateTable(_viewTableName, schema, transaction);
            }
        }

        public void CreateView(string viewName, string viewDefinition, Transaction transaction)
        {
            var tableInfo = _tableManager.GetTableInfo(_viewTableName, transaction);
            var recordFile = new RecordFile(tableInfo, transaction);

            recordFile.Insert();
            recordFile.SetString("viewname", viewName);
            recordFile.SetString("viewdef", viewDefinition);
            recordFile.Close();
        }

        public string GetViewDefinition(string viewName, Transaction transaction)
        {
            var result = string.Empty;
            var tableInfo = _tableManager.GetTableInfo(_viewTableName, transaction);
            var recordFile = new RecordFile(tableInfo, transaction);

            while (recordFile.Next())
            {
                if(recordFile.GetString("viewname") == viewName)
                {
                    result = recordFile.GetString("viewdef");
                    break;
                }
            }

            recordFile.Close();
            return result;
        }
    }
}
