using System;
using System.Diagnostics.CodeAnalysis;

namespace BB.IO.Primitives
{
    public readonly struct Block : IEquatable<Block>
    {
        public readonly int Id { get; }
        public readonly string Filename { get; }

        public Block(string filename, int id)
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
            if (obj == null)
                return false;

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
