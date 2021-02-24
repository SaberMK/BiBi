using BB.Tokenizer.Tokens;
using Superpower;
using Superpower.Model;
using Superpower.Parsers;
using Superpower.Tokenizers;

namespace BB.Tokenizer.Tokenizers
{
    // base class
    public class MetaCommandTokenizer
    {
        private static readonly Tokenizer<MetaCommandToken> _tokenizer =
            new TokenizerBuilder<MetaCommandToken>()
                .Match(Span.EqualToIgnoreCase(MetaKeywords.Close), MetaCommandToken.Close)
                .Match(Span.EqualToIgnoreCase(MetaKeywords.Version), MetaCommandToken.Version)
                .Match(Span.WithAll(x => true), MetaCommandToken.NonMeta)
                .Build();

        public TokenList<MetaCommandToken> Tokenize(string source)
        {
            return _tokenizer.Tokenize(source);
        }
    }
}
