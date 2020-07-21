using System;
using System.Collections.Generic;
using System.Linq;

namespace Sharding
{
    public class ShardSelection
    {
        private static readonly DateTime StartTime = new DateTime(1970, 1, 1, 0, 0, 0);

        private List<KeyValuePair<long, int>> records;
        private ScaleRecordTreeNode rootNode;

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
            int mid = this.records.Count / 2;
            this.rootNode = new ScaleRecordTreeNode
            {
                Record = this.records[mid]
            };
            this.BuildTree(0, this.records.Count - 1, mid, this.rootNode);
        }

        /// <summary>
        /// 构建二叉搜索树
        /// </summary>
        /// <param name="begin"></param>
        /// <param name="end"></param>
        /// <param name="rootIndex"></param>
        /// <param name="rootNode"></param>
        private void BuildTree(int begin, int end, int rootIndex, ScaleRecordTreeNode rootNode)
        {
            if (begin > end || begin > rootIndex || rootIndex > end)
            {
                return;
            }

            if (begin < rootIndex)
            {
                var l = (begin + rootIndex - 1) / 2;
                if (l < this.records.Count)
                {
                    var lNode = new ScaleRecordTreeNode
                    {
                        Record = this.records[l]
                    };
                    rootNode.LeftNode = lNode;
                    this.BuildTree(begin, rootIndex - 1, l, lNode);
                }
            }
            if (rootIndex < end)
            {
                var r = (rootIndex + 1 + end) / 2;
                if (r < this.records.Count)
                {
                    var rNode = new ScaleRecordTreeNode
                    {
                        Record = this.records[r]
                    };
                    rootNode.RightNode = rNode;
                    this.BuildTree(rootIndex + 1, end, r, rNode);
                }
            }
        }

        public int SelectShard(string key)
        {
            return this.SelectShard(key, (long)(DateTime.UtcNow - StartTime).TotalMilliseconds);
        }

        public int SelectShard(string key, long timestamp)
        {
            int shardIndex = 0;
            this.SearchShardScale(timestamp, this.rootNode, ref shardIndex);
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
            this.SearchShardScale(begin, this.rootNode, ref beginIndex);
            this.SearchShardScale(end, this.rootNode, ref endIndex);
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

        private void SearchShardScale(long timestamp, ScaleRecordTreeNode node, ref int result)
        {
            if (node == null)
            {
                return;
            }

            if (timestamp >= node.Record.Key)
            {
                result = node.Record.Value;
                this.SearchShardScale(timestamp, node.RightNode, ref result);
            }
            else
            {
                this.SearchShardScale(timestamp, node.LeftNode, ref result);
            }
        }
    }
}