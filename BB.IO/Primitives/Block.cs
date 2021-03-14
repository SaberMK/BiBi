using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace BB.IO.Primitives
{
    public readonly struct Block : IEquatable<Block>
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
            unchecked
            {
                return Filename.GetHashCode() ^ Id;
            }
        }

        public bool Equals([AllowNull] Block other)
        {
            if (other == null) 
                return false;

            return Id == other.Id && Filename == other.Filename;
        }

        public override bool Equals(object obj)
        {
            return Equals((Block)obj);
        }

        public static bool operator ==(Block obj1, Block obj2)
        {
            return obj1.Equals(obj2);
        }

        public static bool operator !=(Block obj1, Block obj2)
        {
            return !obj1.Equals(obj2);
        }
    }
}
