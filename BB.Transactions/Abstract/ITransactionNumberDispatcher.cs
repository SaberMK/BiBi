﻿using System;
using System.Collections.Generic;
using System.Text;

namespace BB.Transactions.Abstract
{
    public interface ITransactionNumberDispatcher
    {
        public int GetNextTransactionNumber();
    }
}
