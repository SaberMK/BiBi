using BB.Tokenizer.Expressions.Base;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace BB.Tokenizer.Expressions.Meta
{
    public class VersionMetaExpression : MetaExpression
    {
        // TODO - move version somewhere else
        public override void Execute()
        {
            Console.WriteLine($"Version: {Assembly.GetExecutingAssembly().GetName().Version}");
        }
    }
}
