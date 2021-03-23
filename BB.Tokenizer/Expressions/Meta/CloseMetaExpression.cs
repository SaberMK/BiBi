using BB.Tokenizer.Expressions.Base;
using System;

namespace BB.Tokenizer.Expressions.Meta
{
    public class CloseMetaExpression : MetaExpression
    {
        public override void Execute()
        {
            Environment.Exit(1);
        }
    }
}
