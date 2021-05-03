using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BB.Query.Expressions
{
    public interface IConstant
    {
    }

    public class Constant : IConstant { }

    public class Constant<T> : Constant
    {
        public T Value { get; set; }
        public Constant(T value)
        {
            Value = value;
        }
    }
}
