using BB.Metadata.Statistic;
using BB.Transactions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Metadata.Abstract
{
    public interface IStatisticsManager
    {
        StatisticalInfo GetStatisticalInfo(string tableName, Transaction transaction);
    }
}
