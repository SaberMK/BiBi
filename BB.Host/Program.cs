using BB.Tokenizer.Expressions.Meta;
using BB.Tokenizer.Parsers;
using BB.Tokenizer.Tokenizers;
using System;

namespace BB.Host
{
    // TODO: Refactor
    class Program
    {
        private const string Prompt = "bb> ";

        // TODO move it somewhere else or just put away logic
        static void Main(string[] args)
        {
            var line = string.Empty;

            // REPL
            do
            {
                Console.Write(Prompt);
                line = Console.ReadLine();
                if(!string.IsNullOrWhiteSpace(line) && line.Length > 0)
                {
                    var metaExpressions = new MetaCommandTokenizer().Tokenize(line);
                    var metaCommand = new MetaCommandParser().ParseLine(metaExpressions);

                    if(!(metaCommand is NonMetaExpression))
                    {
                        metaCommand.Execute();
                    }
                    else
                    {
                        // tokenize string
                        // and parse
                        var expressions = new MainSqlTokenizer().Tokenize(line);
                        var token = new MainSqlParser().ParseLine(expressions);
                    }
                }
            } while (!string.IsNullOrWhiteSpace(line));
        }
    }
}
