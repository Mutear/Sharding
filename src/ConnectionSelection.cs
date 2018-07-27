using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Sharding
{
    public class ConnectionSelection
    {
        private static readonly DateTime StartTime = new DateTime(1970, 1, 1, 0, 0, 0);

        private ShardSelection shardSelection;
        private List<string> connections;
        private Func<string, IDbConnection> creator;

        public ConnectionSelection(IShardScaleRecord record, List<string> connections,
            Func<string, IDbConnection> creator)
        {
            if(record == null)
            {
                throw new ArgumentNullException(nameof(record));
            }
            if(connections == null)
            {
                throw new ArgumentNullException(nameof(connections));
            }
            if(creator == null)
            {
                throw new ArgumentNullException(nameof(creator));
            }

            this.shardSelection = new ShardSelection(record);
            this.connections = connections;
            this.creator = creator;
        }

        public IDbConnection SelectConnection(string key)
        {
            return this.SelectConnection(key, (long)(DateTime.UtcNow - StartTime).TotalMilliseconds);
        }

        public IDbConnection SelectConnection(string key, long timestamp)
        {
            var shard = this.shardSelection.SelectShard(key, timestamp);
            return this.CreateConnection(shard);
        }

        public IEnumerable<IDbConnection> SelectConnections(string key, long begin, long end)
        {
            var shards = this.shardSelection.SelectShards(key, begin, end);
            if(shards == null)
            {
                return null;
            }

            return shards.Select(shard =>
            {
                if(shard < this.connections.Count)
                {
                    return this.creator.Invoke(this.connections[shard]);
                }

                return null;
            })
            .Where(conn => conn != null);
        }

        public IEnumerable<IDbConnection> SelectConnections(string key)
        {
            var shards = this.shardSelection.SelectShards(key);
            if(shards == null)
            {
                return null;
            }

            return shards.Select(shard =>
            {
                if(shard < this.connections.Count)
                {
                    return this.creator.Invoke(this.connections[shard]);
                }

                return null;
            })
            .Where(conn => conn != null);
        }

        public IDbConnection SelectConnection(long id)
        {
            var shard = this.shardSelection.SelectShard(id);
            return this.CreateConnection(shard);
        }

        private IDbConnection CreateConnection(int shard)
        {
            if(shard < this.connections.Count)
            {
                return this.creator.Invoke(this.connections[shard]);
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(shard), 
                    $"The shard {shard} is calculated through parameter(s), but the count of connections is {this.connections.Count}.");
            }
        }
    }
}