
namespace PartialUpdateOptions
{
    public class PartialUpdateMergeOptions
    {
        /// <summary>
        /// Array Merge Options
        /// </summary>
        public ArrayMergeOptions ArrayMergeOptions { get; set; }

        /// <summary>
        /// Null Value Merge options
        /// </summary>
        public NullValueMergeOptions NullValueMergeOptions { get; set; }

        /// <summary>
        /// Object Merge Options
        /// </summary>
        public ObjectMergeOptions ObjectMergeOptions { get; set; }

        /// <summary>
        /// If a nested object or an object within an array of objects needs to be update,
        /// this field is used to identify the object to be updated.
        /// </summary>
        public string objectFilteringPropertyName { get; set; }

        /// <summary>
        /// If a nested object or an object within an array of objects needs to be update,
        /// this is the value of the filtering field (objectFilteringPropertyName) 
        /// used to identify the object to be updated.
        /// </summary>
        public string objectFilteringPropertyValue { get; set; }

        /// <summary>
        /// Get the default partial update merge options to use when merge options are not specified by the caller
        /// </summary>
        /// <returns></returns>
        public static PartialUpdateMergeOptions GetDefaultPartialUpdateMergeOptions()
        {
            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.UNION;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.UPDATE;

            return partialUpdateMergeOptions;
        }
    }
}
