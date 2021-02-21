using System;

namespace BB.Host
{
    class Program
    {
        private const string Prompt = "bb> ";

        static void Main(string[] args)
        {
            var line = string.Empty;

            // repl
            do
            {
                Console.Write(Prompt);
                line = Console.ReadLine();
                // tokenize string
                // and parse



            } while (!string.IsNullOrWhiteSpace(line));
        }
    }
}
