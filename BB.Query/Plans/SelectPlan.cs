using BB.Query.Abstract;
using BB.Query.Expressions;
using BB.Query.Scans;
using BB.Record.Base;
using System;

namespace BB.Query.Plans
{
    public class SelectPlan : IPlan
    {
        private readonly IPlan _plan;
        private readonly Predicate _predicate;

        public SelectPlan(IPlan plan, Predicate predicate)
        {
            _plan = plan;
            _predicate = predicate;
        }

        public IScan Open()
        {
            var scan = _plan.Open();
            return new SelectScan(scan, _predicate);
        }

        public int BlocksAccessed => _plan.BlocksAccessed;

        public int RecordsOutput => _plan.RecordsOutput / _predicate.ReductionFactor(_plan);

        public Schema Schema => _plan.Schema;

        public int DistinctValues(string fieldName)
        {
            if (_predicate.EquaresWithConstant(fieldName) != null)
                return 1;

            return Math.Min(_plan.DistinctValues(fieldName), RecordsOutput);
        }
    }
}
