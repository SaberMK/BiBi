using BB.Transactions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Metadata.Abstract
{
    public interface IViewManager
    {
        void CreateView(string viewName, string viewDefinition, Transaction transaction);
        string GetViewDefinition(string viewName, Transaction transaction);
    }
}
