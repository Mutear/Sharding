using System.Collections.Generic;

namespace Sharding
{
    public class ScaleRecordTreeNode
    {
        public KeyValuePair<long, int> Record{get;set;}

        public ScaleRecordTreeNode LeftNode{get;set;}

        public ScaleRecordTreeNode RightNode{get;set;}
    }
}