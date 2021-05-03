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
    public class ProjectPlan : IPlan
    {
        private readonly IPlan _plan;
        private readonly Schema _schema;

        public ProjectPlan(IPlan plan, ICollection<string> fieldsCollection)
        {
            _plan = plan;
            _schema = new Schema();

            foreach(var fieldName in fieldsCollection)
            {
                _schema.Add(fieldName, plan.Schema);
            }
        }

        public IScan Open()
        {
            var scan = _plan.Open();
            return new ProjectScan(scan, _schema.Fields.Keys);
        }

        public int BlocksAccessed => _plan.BlocksAccessed;

        public int RecordsOutput => _plan.RecordsOutput;

        public Schema Schema => _schema;

        public int DistinctValues(string fieldName) => _plan.DistinctValues(fieldName);
    }
}
