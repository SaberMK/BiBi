using BB.Query.Abstract;
using BB.Record.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Expressions
{
    public class Predicate
    {
        private readonly List<Term> _terms = new List<Term>();

        public Predicate()
        { }

        public Predicate(Term term)
        {
            _terms.Add(term);
        }

        public void ConjoinWith(Predicate predicate)
        {
            _terms.AddRange(predicate.Terms);
        }

        public bool IsSatisfied(IScan scan)
        {
            foreach(var term in _terms)
            {
                if (!term.IsSatisfied(scan))
                    return false;
            }
            return true;
        }

        public int ReductionFactor(IPlan plan)
        {
            int factor = 1;

            foreach(var term in _terms)
            {
                factor *= term.ReductionFactor(plan);
            }

            return factor;
        }

        public Predicate SelectPredicate(Schema schema)
        {
            var result = new Predicate();

            foreach(var term in _terms)
            {
                if(term.AppliesTo(schema))
                {
                    result.Terms.Add(term);
                }
            }

            if (result.Terms.Count == 0)
                return null;

            return result;
        }

        public Predicate JoinPredicate(Schema schema1, Schema schema2)
        {
            var result = new Predicate();
            
            var joinedSchema = new Schema();
            joinedSchema.AddAll(schema1);
            joinedSchema.AddAll(schema2);

            foreach(var term in _terms)
            {
                if(!term.AppliesTo(schema1) && !term.AppliesTo(schema2) && term.AppliesTo(joinedSchema))
                {
                    result.Terms.Add(term);
                }
            }

            if (result.Terms.Count == 0)
                return null;

            return result;
        }

        public Constant EquaresWithConstant(string fieldName)
        {
            foreach(var term in _terms)
            {
                var constant = term.EquatesWithConstant(fieldName);
                if (constant != null)
                    return constant;
            }

            return null;
        }

        public string EquatesWithField(string fieldName)
        {
            foreach (var term in _terms)
            {
                var constant = term.EquatesWithField(fieldName);
                if (constant != null)
                    return constant;
            }

            return null;
        }

        public override string ToString()
        {
            if (_terms.Count == 0)
                return "";

            return string.Join(" AND ", _terms.Select(x => x.ToString()));
        }

        public List<Term> Terms => _terms;
    }
}
