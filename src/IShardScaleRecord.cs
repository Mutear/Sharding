using System.Collections.Generic;

namespace Sharding
{
    public interface IShardScaleRecord
    {
        IEnumerable<KeyValuePair<long, int>> GetRecords();
    }
}