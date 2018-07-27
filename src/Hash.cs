namespace Sharding
{
    public static class Hash
    {
        public static int GetHashCode(string key)
        {
            int h = 0;

            if(!string.IsNullOrEmpty(key))
            {
                foreach(var ch in key)
                {
                    h = 31 * h + ch;
                }
            }

            return h;
        }
    }
}