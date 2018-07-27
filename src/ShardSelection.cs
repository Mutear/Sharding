using System;
using System.Collections.Generic;
using System.Linq;

namespace Sharding
{
    public class ShardSelection
    {
        private static readonly DateTime StartTime = new DateTime(1970, 1, 1, 0, 0, 0);

        private List<KeyValuePair<long, int>> records;
        private int[] tree;

        public ShardSelection(IShardScaleRecord record)
        {
            if(record == null)
            {
                throw new ArgumentNullException(nameof(record));
            }

            var rs = record.GetRecords();
            if(rs == null || rs.Count() <= 0)
            {
                throw new Exception("Cannot get records from IShardScaleRecord");
            }

            this.records = rs.OrderBy(pair => pair.Key).ToList();
            this.tree = new int[this.records.Count];
            int mid = this.records.Count / 2;
            this.tree[0] = mid;
            this.BuildTree(0, 0, this.records.Count - 1, mid);
        }

        private void BuildTree(int index, int begin, int end, int root)
        {
            if(index >= this.tree.Length)
            {
                return;
            }
            if(begin > end || begin > root || root > end)
            {
                return;
            }

            int leftIndex = 2 * index + 1;
            int rightIndex = 2 * index + 2;

            if(leftIndex < this.tree.Length)
            {
                int r = (begin + root - 1) / 2;
                this.tree[leftIndex] = r;
                this.BuildTree(leftIndex, begin, root - 1, r);
            }

            if(rightIndex < this.tree.Length)
            {
                int r = (root + 1 + end) / 2;
                this.tree[rightIndex] = r;
                this.BuildTree(rightIndex, root + 1, end, r);
            }
        }

        public int SelectShard(string key)
        {
            return this.SelectShard(key, (long)(DateTime.UtcNow - StartTime).TotalMilliseconds);
        }

        public int SelectShard(string key, long timestamp)
        {
            int shardIndex = 0;
            this.SearchShardScale(timestamp, 0, ref shardIndex);
            int hash = Hash.GetHashCode(key);
            int shard = hash % this.records[shardIndex].Value;
            if(shard < 0)
            {
                shard += this.records[shardIndex].Value;
            }
            return shard;
        }

        public IEnumerable<int> SelectShards(string key, long begin, long end)
        {
            int beginIndex = 0, endIndex = 0;
            this.SearchShardScale(begin, 0, ref beginIndex);
            this.SearchShardScale(end, 0, ref endIndex);
            int hash = Hash.GetHashCode(key);

            var result = new List<int>();
            for(int i = beginIndex; i <= endIndex; i++)
            {
                int shard = hash % this.records[i].Value;
                if(shard < 0)
                {
                    shard += this.records[i].Value;
                }
                result.Add(shard);
            }
            return result.Distinct();
        }

        public IEnumerable<int> SelectShards(string key)
        {
            int hash = Hash.GetHashCode(key);
            var result = new List<int>();
            for(int i = 0; i < this.records.Count; i++)
            {
                int shard = hash % this.records[i].Value;
                if(shard < 0)
                {
                    shard += this.records[i].Value;
                }
                result.Add(shard);
            }
            return result.Distinct();
        }

        public int SelectShard(long id)
        {
            return (int)((id >> IdWorker.SequenceBits) & IdWorker.ShardingMask);
        }

        private void SearchShardScale(long timestamp, int index, ref int result)
        {
            if(index >= this.tree.Length)
            {
                return;
            }

            var root = this.records[this.tree[index]];
            long comparor = root.Key;
            if(timestamp >= comparor)
            {
                result = this.tree[index];
                this.SearchShardScale(timestamp, 2 * index + 2, ref result);
            }
            else
            {
                this.SearchShardScale(timestamp, 2 * index + 1, ref result);
            }
        }
    }
}