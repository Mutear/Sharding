using System.Collections.Generic;

namespace Sharding
{
    /// <summary>
    /// 规模变更记录
    /// <para>key 为 utc 时间戳，value 为规模大小</para>
    /// </summary>
    public interface IShardScaleRecord
    {
        IEnumerable<KeyValuePair<long, int>> GetRecords();
    }
}