using BB.Query.Abstract;
using BB.Query.Scans;
using BB.Record.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Plans
{
    public class ProductPlan : IPlan
    {
        private readonly IPlan _plan1;
        private readonly IPlan _plan2;
        private readonly Schema _schema;

        public ProductPlan(IPlan plan1, IPlan plan2)
        {
            _plan1 = plan1;
            _plan2 = plan2;

            _schema = new Schema();
            _schema.AddAll(_plan1.Schema);
            _schema.AddAll(_plan2.Schema);
        }

        public IScan Open()
        {
            var scan1 = _plan1.Open();
            var scan2 = _plan2.Open();
            return new ProductScan(scan1, scan2);
        }

        public int BlocksAccessed => _plan1.BlocksAccessed + _plan1.RecordsOutput * _plan2.BlocksAccessed;

        public int RecordsOutput => _plan1.RecordsOutput * _plan2.RecordsOutput;

        public int DistinctValues(string fieldName)
        {
            if (_plan1.Schema.HasField(fieldName))
                return _plan1.DistinctValues(fieldName);

            if (_plan2.Schema.HasField(fieldName))
                return _plan2.DistinctValues(fieldName);

            return int.MaxValue;
        }

        public Schema Schema => _schema;
    }
}
