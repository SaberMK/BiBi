using BB.Tokenizer.Tokens;
using Superpower;
using Superpower.Model;
using Superpower.Parsers;
using Superpower.Tokenizers;
using System.Collections.Generic;

namespace BB.Tokenizer.Tokenizers
{
    // Assign everything to interfaces
    // Still would need to rewrite it manually...
    public class MainSqlTokenizer
    {
        private static readonly Tokenizer<MainSqlToken> _tokenizer =
            new TokenizerBuilder<MainSqlToken>()
                .Match(Span.EqualToIgnoreCase(Keywords.SELECT), MainSqlToken.Select)
                .Match(Span.EqualToIgnoreCase(Keywords.UPDATE), MainSqlToken.Update)
                .Match(Span.WithAll(x => char.IsWhiteSpace(x)), MainSqlToken.None)
                .Build();

        public TokenList<MainSqlToken> Tokenize(string source)
        {
            return _tokenizer.Tokenize(source);
        }
    }
}
