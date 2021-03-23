using BB.Tokenizer.Expressions.Base;
using BB.Tokenizer.Expressions.Meta;
using BB.Tokenizer.Tokens;
using Superpower;
using Superpower.Model;
using Superpower.Parsers;

namespace BB.Tokenizer.Parsers
{
    public class MetaCommandParser
    {
        private static readonly TokenListParser<MetaCommandToken, MetaExpression> Close =
            Token.EqualTo(MetaCommandToken.Close)
                .Apply(Span.EqualToIgnoreCase(MetaKeywords.Close))
                .Select(x => new CloseMetaExpression() as MetaExpression);


        private static readonly TokenListParser<MetaCommandToken, MetaExpression> Version =
            Token.EqualTo(MetaCommandToken.Version)
                .Apply(Span.EqualToIgnoreCase(MetaKeywords.Version))
                .Select(x => new VersionMetaExpression() as MetaExpression);

        private static readonly TokenListParser<MetaCommandToken, MetaExpression> NonMeta =
            Token.EqualTo(MetaCommandToken.NonMeta)
                .Select(x => new NonMetaExpression() as MetaExpression);

        private static TokenListParser<MetaCommandToken, MetaExpression> Expression =
            Close
                .Or(Version)
                .Or(NonMeta);

        // mb in future there would be multi-line expressions, but for now... meeeh
        public MetaExpression ParseLine(TokenList<MetaCommandToken> input) => Expression.Parse(input);
    }
}
