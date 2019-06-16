
namespace PartialUpdateOptions
{
    public enum ArrayMergeOptions
    {
        /// <summary>
        /// Simply merges the existing array with the array provided in the partial update
        /// </summary>
        MERGE,

        /// <summary>
        /// Similar to MERGE, where the array provided in the partial update is added to the existing contents of the array in the document
        /// </summary>
        CONCAT,

        /// <summary>
        /// Performs a UNION operation of the existing array and the array provided in the partial update.
        /// This ensures duplicates in the partial update array are not merged into the existing array.
        /// </summary>
        UNION,

        /// <summary>
        /// Simply replaces the entire array with the array provided in the partial update
        /// </summary>
        REPLACE
    }
}
