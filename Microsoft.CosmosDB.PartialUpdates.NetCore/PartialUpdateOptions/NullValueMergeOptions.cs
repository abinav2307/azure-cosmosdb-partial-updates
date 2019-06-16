
namespace PartialUpdateOptions
{
    public enum NullValueMergeOptions
    {
        /// <summary>
        /// This merge option ignores null values while performing the partial update
        /// </summary>
        IGNORE,

        /// <summary>
        /// This merge option includes nulls while performing the partial update
        /// </summary>
        MERGE
    }
}
