using System;
using Sharding;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            var selection = new ShardSelection(new ShardScaleRecord());
            Console.WriteLine(selection.SelectShard("test", 222227));
        }
    }
}
