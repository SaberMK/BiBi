using BB.Tokenizer.Expressions.Base;
using BB.Tokenizer.Expressions.Sql;
using BB.Tokenizer.Tokens;
using Superpower;
using Superpower.Model;
using Superpower.Parsers;

namespace BB.Tokenizer.Parsers
{
    public class MainSqlParser
    {
        private static readonly TokenListParser<MainSqlToken, SqlExpression> Select =
                Token.EqualTo(MainSqlToken.Select)
                    .Apply(Span.EqualToIgnoreCase(Keywords.SELECT))
                    .Select(x => new SelectExpression() as SqlExpression);

        private static readonly TokenListParser<MainSqlToken, SqlExpression> Update =
                Token.EqualTo(MainSqlToken.Update)
                    .Apply(Span.EqualToIgnoreCase(Keywords.UPDATE))
                    .Select(x => new UpdateExpression() as SqlExpression);

        private static readonly TokenListParser<MainSqlToken, SqlExpression> None =
                Token.EqualTo(MainSqlToken.None)
                    .Apply(Span.WithAll(x => char.IsWhiteSpace(x)))
                    .Select(x => new NoneExpression() as SqlExpression);

        private static TokenListParser<MainSqlToken, SqlExpression> Expression =
             None
                .Or(Select)
                .Or(Update);

        // mb in future there would be multi-line expressions, but for now... meeeh
        public SqlExpression ParseLine(TokenList<MainSqlToken> input) => Expression.Parse(input);
    }
}
