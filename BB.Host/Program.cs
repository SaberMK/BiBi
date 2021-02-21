using BB.Tokenizer.Parsers;
using BB.Tokenizer.Tokenizers;
using System;

namespace BB.Host
{
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
                if (line.ToLower() == "exit")
                    break;



                // tokenize string
                // and parse
                var expressions = new MainSqlTokenizer().Tokenize(line);
                var tokens = new MainSqlParser().ParseLine(expressions);


            } while (!string.IsNullOrWhiteSpace(line));
        }
    }
}
