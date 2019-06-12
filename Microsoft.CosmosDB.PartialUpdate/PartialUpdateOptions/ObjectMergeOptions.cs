
namespace PartialUpdateOptions
{
    public enum ObjectMergeOptions
    {
        /// <summary>
        /// Replaces the entire object with the object provided in the partial update
        /// </summary>
        REPLACE,

        /// <summary>
        /// Simply updates the existing object with the contents of the object provided in the partial update
        /// </summary>
        UPDATE
    }
}
