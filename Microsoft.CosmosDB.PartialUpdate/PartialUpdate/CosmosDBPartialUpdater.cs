
namespace Microsoft.Azure.CosmosDB.PartialUpdate
{
    using Microsoft.Azure.CosmosDB.CosmosDBPartialUpdate;

    using PartialUpdateOptions;
    using Newtonsoft.Json.Linq;

    using System;
    using System.Threading.Tasks;
    using System.Collections.Generic;

    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;

    public class CosmosDBPartialUpdater
    {
        private int MaxRetriesOnThrottledAttempts = 10;

        private PartialUpdateMergeOptions MergeOptions;

        private DocumentClient DocumentClient;

        public CosmosDBPartialUpdater(DocumentClient client)
        {
            this.DocumentClient = client;
        }

        /// <summary>
        /// Execute a partial update of the document specified by the partition key and id. 
        /// The partial document is used in conjunction with the specified Merge Options to perform the partial update
        /// </summary>
        /// <param name="databaseName">Azure Cosmos DB Database containing the document to be updated</param>
        /// <param name="collectionName">Azure Cosmos DB collection containing the document to be updated</param>
        /// <param name="partitionKey">Partition key of the document to be partially updated</param>
        /// <param name="id">id of the document to be partially updated</param>
        /// <param name="partialUpdateDocument">Document containing the partial updates</param>
        /// <param name="partialUpdateMergeOptions">Merge options specifying the parameters of the partial update</param>
        public async Task<List<ResourceResponse<Document>>> ExecutePartialUpdate(
            string databaseName,
            string collectionName,
            string queryText,
            JObject partialUpdateDocument,
            string partitionKey = null,
            PartialUpdateMergeOptions partialUpdateMergeOptions = null)
        {
            // Null partition key error handling
            if (string.IsNullOrEmpty(queryText))
            {
                throw new ArgumentException("Query text cannot be null or empty");
            }

            // Validate the inputs to the Partial Update method and set the default PartialUpdateMergeOptions if needed
            this.ValidateInputs(databaseName, collectionName, partialUpdateDocument, partialUpdateMergeOptions);

            // Retrieve the documents to be updated
            List<Document> documentsMatchingQuery = await CosmosDBHelper.QueryDocuments(this.DocumentClient, databaseName, collectionName, queryText, this.MaxRetriesOnThrottledAttempts);

            List<ResourceResponse<Document>> partialUpdateResponse = new List<ResourceResponse<Document>>();

            foreach (Document eachDocumenToUpdate in documentsMatchingQuery)
            {
                partialUpdateResponse.Add(await this.ExecuteUpdate(
                    databaseName,
                    collectionName,
                    JObject.Parse(eachDocumenToUpdate.ToString()),
                    partialUpdateDocument,
                    partialUpdateMergeOptions));
            }

            return partialUpdateResponse;
        }

        /// <summary>
        /// Execute a partial update of the document specified by the partition key and id. 
        /// The partial document is used in conjunction with the specified Merge Options to perform the partial update
        /// </summary>
        /// <param name="databaseName">Azure Cosmos DB Database containing the document to be updated</param>
        /// <param name="collectionName">Azure Cosmos DB collection containing the document to be updated</param>
        /// <param name="partitionKey">Partition key of the document to be partially updated</param>
        /// <param name="id">id of the document to be partially updated</param>
        /// <param name="partialUpdateDocument">Document containing the partial updates</param>
        /// <param name="partialUpdateMergeOptions">Merge options specifying the parameters of the partial update</param>
        public async Task<ResourceResponse<Document>> ExecutePartialUpdate(
            string databaseName,
            string collectionName,
            string partitionKey,
            string id,
            JObject partialUpdateDocument,
            PartialUpdateMergeOptions partialUpdateMergeOptions = null)
        {
            // Null partition key error handling
            if (string.IsNullOrEmpty(partitionKey))
            {
                throw new ArgumentException("Partition key cannot be null or empty");
            }

            // Null document id error handling
            if (string.IsNullOrEmpty(id))
            {
                throw new ArgumentException("Document id cannot be null or empty");
            }

            // Validate the inputs to the Partial Update method and set the default PartialUpdateMergeOptions if needed
            this.ValidateInputs(databaseName, collectionName, partialUpdateDocument, partialUpdateMergeOptions);

            // Retrieve the document to be partially updated
            Document documentToPartiallyUpdate = await CosmosDBHelper.ReadDocmentAsync(this.DocumentClient, databaseName, collectionName, partitionKey, id, this.MaxRetriesOnThrottledAttempts);

            if (documentToPartiallyUpdate == null)
            {
                throw new ArgumentException("Document with specified partition key and id does not exist");
            }
            else
            {
                return await this.ExecuteUpdate(
                    databaseName,
                    collectionName,
                    JObject.Parse(documentToPartiallyUpdate.ToString()), 
                    partialUpdateDocument, 
                    partialUpdateMergeOptions);
            }
        }

        /// <summary>
        /// Validates the inputs to the partial update method
        /// </summary>
        /// <param name="databaseName">Azure Cosmos DB database containing the document to be updated</param>
        /// <param name="collectionName">Azure Cosmos DB collection containing the document to be updated</param>
        /// <param name="partialUpdateDocument">The document containing the updates to be made</param>
        /// <param name="partialUpdateMergeOptions">Merge Options specifying the nature of the updates</param>
        private void ValidateInputs(string databaseName, string collectionName, JObject partialUpdateDocument, PartialUpdateMergeOptions partialUpdateMergeOptions)
        {
            // Null partition key error handling
            if (string.IsNullOrEmpty(databaseName))
            {
                throw new ArgumentException("Database name key cannot be null or empty");
            }

            // Null document id error handling
            if (string.IsNullOrEmpty(collectionName))
            {
                throw new ArgumentException("Collection name cannot be null or empty");
            }

            // Null partial update error handling
            if (partialUpdateDocument == null)
            {
                throw new ArgumentException("partialUpdateDocument cannot be null");
            }

            // Set the default partial updates
            if (partialUpdateMergeOptions != null)
            {
                // A filtering property name (to determine the nested object to update), cannot be null if 
                // a value has been provided for the filtering property name as part of PartialUpdateMergeOptions
                if (string.IsNullOrEmpty(partialUpdateMergeOptions.objectFilteringPropertyName) && !string.IsNullOrEmpty(partialUpdateMergeOptions.objectFilteringPropertyValue))
                {
                    throw new ArgumentException("objectFilteringPropertyName cannot be null if objectFilteringPropertyValue is NOT null");
                }

                MergeOptions = partialUpdateMergeOptions;
            }
            else
            {
                MergeOptions = PartialUpdateMergeOptions.GetDefaultPartialUpdateMergeOptions();
            }
        }

        /// <summary>
        /// This method finds the specific object within the document which contains the fields to be updated
        /// </summary>
        /// <param name="documentToUpdate">The document retrieved from Azure Cosmos DB to be updated</param>
        /// <param name="partialUpdateMergeOptions">Merge Options specifying the nature of the update operation</param>
        /// <returns></returns>
        private JObject FindObjectToUpdate(JObject documentToUpdate, PartialUpdateMergeOptions partialUpdateMergeOptions)
        {
            if (partialUpdateMergeOptions.objectFilteringPropertyName == null)
            {
                return documentToUpdate;
            }

            if (documentToUpdate != null)
            {
                string documentsFilterValue = (string)documentToUpdate[partialUpdateMergeOptions.objectFilteringPropertyName];

                // If the object passed in is not null, has the filtering property field and the value of the filtering property 
                // is in fact equal to the value specified in PartialUpdateMergeOptions, return the object
                if (documentToUpdate[partialUpdateMergeOptions.objectFilteringPropertyName] != null &&
                   ((string)documentToUpdate[partialUpdateMergeOptions.objectFilteringPropertyName]).Equals(partialUpdateMergeOptions.objectFilteringPropertyValue))
                {
                    return documentToUpdate;
                }

                // Since the current object does not have the specified field,
                // iterate through all properties in the Object and recursively search the filtering property in nested objects or arrays
                foreach (JProperty eachPropertyInTheObject in documentToUpdate.Properties())
                {
                    if (eachPropertyInTheObject.Value != null)
                    {
                        if (eachPropertyInTheObject.Value.Type == JTokenType.Object)
                        {
                            JObject objectSatisfyingFilteringCriteria = FindObjectToUpdate((JObject)eachPropertyInTheObject.Value, partialUpdateMergeOptions);

                            if (objectSatisfyingFilteringCriteria != null)
                            {
                                return objectSatisfyingFilteringCriteria;
                            }
                        }
                        else if (eachPropertyInTheObject.Value.Type == JTokenType.Array)
                        {
                            foreach (JToken eachChildTokenInJArray in ((JArray)eachPropertyInTheObject.Value).Children())
                            {
                                if(eachChildTokenInJArray.Type == JTokenType.Object)
                                {
                                    JObject eachJObjectInJArray = (JObject)eachChildTokenInJArray;

                                    JObject objectSatisfyingFilteringCriteria = FindObjectToUpdate(eachJObjectInJArray, partialUpdateMergeOptions);

                                    if (objectSatisfyingFilteringCriteria != null)
                                    {
                                        return objectSatisfyingFilteringCriteria;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Executes the partial update and persists the changes in Cosmos DB
        /// </summary>
        /// <param name="databaseName">Azure Cosmos DB database containing the document to be updated</param>
        /// <param name="collectionName">Azure Cosmos DB collection containing the document to be updated</param>
        /// <param name="documentToUpdate">Document on which the partial updates were applied</param>
        /// <param name="partialUpdateDocument">JObject containing the partial updates</param>
        /// <param name="partialUpdateMergeOptions">Merge Options specifying the nature of the updates</param>
        /// <returns></returns>
        public async Task<ResourceResponse<Document>> ExecuteUpdate(string databaseName, string collectionName, JObject documentToUpdate, JObject partialUpdateDocument, PartialUpdateMergeOptions partialUpdateMergeOptions)
        {
            // Retrieve the object within the document to update
            JObject objectToUpdate = FindObjectToUpdate(documentToUpdate, partialUpdateMergeOptions);

            if(objectToUpdate == null)
            {
                throw new ArgumentException(
                    string.Format("Object with filter: {0} and value: {1} does not exist.", 
                    partialUpdateMergeOptions.objectFilteringPropertyName, 
                    partialUpdateMergeOptions.objectFilteringPropertyValue));
            }

            if (partialUpdateMergeOptions.ObjectMergeOptions == ObjectMergeOptions.REPLACE)
            {
                // First, remove all the properties of this document since the user has chosen to replace the
                // existing object, with the contents of the object in the partial update payload
                List<JToken> propertiesToRemove = new List<JToken>();
                foreach (JToken eachPropertyInTheObject in objectToUpdate.Properties())
                {
                    propertiesToRemove.Add(eachPropertyInTheObject);                    
                }
                foreach (JToken eachPropertyToRemove in propertiesToRemove)
                {
                    eachPropertyToRemove.Remove();
                }
            }

            foreach (JProperty eachPropertyToUpdate in partialUpdateDocument.Properties())
            {
                if(eachPropertyToUpdate.Value != null)
                {
                    if (eachPropertyToUpdate.Value.Type == JTokenType.Array)
                    {
                        ExecuteArrayMerge(objectToUpdate, partialUpdateDocument, eachPropertyToUpdate.Name, partialUpdateMergeOptions);
                    }
                    else if (eachPropertyToUpdate.Value.Type == JTokenType.Object)
                    {
                        ExecuteObjectMerge(objectToUpdate, partialUpdateDocument, eachPropertyToUpdate.Name, partialUpdateMergeOptions);
                    }
                    else
                    {
                        objectToUpdate[eachPropertyToUpdate.Name] = partialUpdateDocument[eachPropertyToUpdate.Name];
                    }
                }
            }

            return await CosmosDBHelper.UpsertDocumentAsync(this.DocumentClient, databaseName, collectionName, documentToUpdate, this.MaxRetriesOnThrottledAttempts);
        }

        /// <summary>
        /// Executes an update of an object within the document
        /// </summary>
        /// <param name="documentToUpdate">Object within the document to be updated</param>
        /// <param name="mergeDocument">Document containing the partial updates</param>
        /// <param name="objectPropertyName">The name of the object within the parent document, to be updated</param>
        /// <param name="partialUpdateMergeOptions">Merge Options specifying the nature of the updates</param>
        private void ExecuteObjectMerge(JObject documentToUpdate, JObject mergeDocument, string objectPropertyName, PartialUpdateMergeOptions partialUpdateMergeOptions)
        {
            // If the object to update does not contain the object to be merged, simply add the object into the object to be merged
            if (documentToUpdate[objectPropertyName] == null)
            {
                documentToUpdate[objectPropertyName] = mergeDocument[objectPropertyName];
            }
            else if (partialUpdateMergeOptions.ObjectMergeOptions == ObjectMergeOptions.REPLACE)
            {
                documentToUpdate[objectPropertyName] = mergeDocument[objectPropertyName];
            }
            else if (partialUpdateMergeOptions.ObjectMergeOptions == ObjectMergeOptions.UPDATE)
            {
                foreach (JProperty eachPropertyToUpdate in mergeDocument.Properties())
                {
                    if (eachPropertyToUpdate.Value != null)
                    {
                        documentToUpdate[eachPropertyToUpdate] = mergeDocument[eachPropertyToUpdate];
                    }
                }
            }
        }

        /// <summary>
        /// Executes an update of array within the document
        /// </summary>
        /// <param name="documentToUpdate">Object within the document to be updated</param>
        /// <param name="mergeDocument">Document containing the partial updates</param>
        /// <param name="arrayPropertyName">The name of the array within the parent document, to be updated</param>
        /// <param name="partialUpdateMergeOptions">Merge Options specifying the nature of the updates</param>
        private void ExecuteArrayMerge(JObject documentToUpdate, JObject mergeDocument, string arrayPropertyName, PartialUpdateMergeOptions partialUpdateMergeOptions)
        {
            if (partialUpdateMergeOptions.ArrayMergeOptions == ArrayMergeOptions.CONCAT ||
                partialUpdateMergeOptions.ArrayMergeOptions == ArrayMergeOptions.MERGE)
            {
                // If the object to update does not contain the array to be merged, simply add the array property and its value to the object
                if (documentToUpdate[arrayPropertyName] == null)
                {
                    documentToUpdate[arrayPropertyName] = mergeDocument[arrayPropertyName];
                }
                else
                {
                    JArray mergedOrConcatenatedJArray = new JArray();

                    // Add all the elements in the array of the original document
                    foreach (JToken eachElementInOriginalArray in ((JArray)documentToUpdate[arrayPropertyName]).Children())
                    {
                        mergedOrConcatenatedJArray.Add(eachElementInOriginalArray);
                    }

                    // Add all the elements in the array of the merge document
                    foreach (JToken eachElementInMergeDocumentArray in ((JArray)mergeDocument[arrayPropertyName]).Children())
                    {
                        mergedOrConcatenatedJArray.Add(eachElementInMergeDocumentArray);
                    }

                    documentToUpdate[arrayPropertyName] = mergedOrConcatenatedJArray;
                }
            }
            else if (partialUpdateMergeOptions.ArrayMergeOptions == ArrayMergeOptions.UNION)
            {
                // If the object to update does not contain the array to be merged, simply add the array property and its value to the object
                if (documentToUpdate[arrayPropertyName] == null)
                {
                    documentToUpdate[arrayPropertyName] = mergeDocument[arrayPropertyName];
                }
                else
                {
                    // For primitive arrays of numbers, 
                    // simply create a Hashset<Double> and merge the result array back into the object to be updated
                    HashSet<object> mergedArray = new HashSet<object>();

                    // Add all the elements in the original array into the HashSet
                    foreach (JToken eachElementInOriginalArray in ((JArray)documentToUpdate[arrayPropertyName]).Children())
                    {
                        if (eachElementInOriginalArray.Type == JTokenType.Integer || eachElementInOriginalArray.Type == JTokenType.Float)
                        {
                            mergedArray.Add(eachElementInOriginalArray.ToObject<double>());
                        }
                        else if (eachElementInOriginalArray.Type == JTokenType.String)
                        {
                            mergedArray.Add(eachElementInOriginalArray.ToObject<string>());
                        }
                        else
                        {
                            mergedArray.Add(eachElementInOriginalArray.ToObject<object>());
                        }
                    }

                    // Add all the elements in the array within the partial update document into the HashSet
                    foreach (JToken eachElementInMergeDocument in ((JArray)mergeDocument[arrayPropertyName]).Children())
                    {
                        if (eachElementInMergeDocument.Type == JTokenType.Integer || eachElementInMergeDocument.Type == JTokenType.Float)
                        {
                            mergedArray.Add(eachElementInMergeDocument.ToObject<double>());
                        }
                        else if (eachElementInMergeDocument.Type == JTokenType.String)
                        {
                            mergedArray.Add(eachElementInMergeDocument.ToObject<string>());
                        }
                        else
                        {
                            mergedArray.Add(eachElementInMergeDocument.ToObject<object>());
                        }
                    }

                    JArray objectList = new JArray();
                    foreach (object eachObject in mergedArray)
                    {
                        objectList.Add(eachObject);
                    }

                    documentToUpdate[arrayPropertyName] = objectList;
                }
            }
            else if (partialUpdateMergeOptions.ArrayMergeOptions == ArrayMergeOptions.REPLACE)
            {
                documentToUpdate[arrayPropertyName] = mergeDocument[arrayPropertyName];
            }
        }       
    }
}
