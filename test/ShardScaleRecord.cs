using System.Collections.Generic;
using Sharding;

namespace Test
{
    public class ShardScaleRecord : IShardScaleRecord
    {
        public IEnumerable<KeyValuePair<long, int>> GetRecords()
        {
            return new List<KeyValuePair<long, int>>
            {
                new KeyValuePair<long, int>(11111, 1),
                new KeyValuePair<long, int>(222222, 2),
                new KeyValuePair<long, int>(222223, 3),
                new KeyValuePair<long, int>(222224, 4),
                new KeyValuePair<long, int>(222225, 5),
                new KeyValuePair<long, int>(222226, 6)
            };
        }
    }
}