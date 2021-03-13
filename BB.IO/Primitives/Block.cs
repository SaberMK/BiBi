using System;
using System.Collections.Generic;
using System.Text;

namespace BB.IO.Primitives
{
    public readonly struct Block
    {
        public readonly int Id { get; }
        public readonly string Filename { get; }

        public Block(int id, string filename)
        {
            Id = id;
            Filename = filename;
        }

        public override string ToString()
        {
            return $"[file: {Filename}, block id: {Id}]";
        }

        public override int GetHashCode()
        {
            return Filename.GetHashCode() ^ Id;
        }
    }
}
