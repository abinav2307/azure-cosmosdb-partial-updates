
namespace Microsoft.Azure.CosmosDB.CosmosDBPartialUpdate
{
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents;

    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public class CosmosDBHelper
    {
        /// <summary>
        /// Reads a document from the specified Cosmos DB collection and retries when rate limited
        /// </summary>
        /// <param name="client">DocumentClient instance to interact with Azure Cosmos DB</param>
        /// <param name="databaseName">Database name of the collection containing the document to read</param>
        /// <param name="collectionName">Collection name containing the document</param>
        /// <param name="partitionKey">Partition key of the document to read</param>
        /// <param name="id">Id property of the document to read</param>
        /// <param name="maxRetriesOnDocumentClientExceptions">Maximum number of retries when rate limited</param>
        /// <returns></returns>
        public static async Task<ResourceResponse<Document>> ReadDocmentAsync(
            DocumentClient client,
            string databaseName,
            string collectionName,
            string partitionKey,
            string id,
            int maxRetriesOnDocumentClientExceptions)
        {
            int numRetries = 0;
            Uri documentsLink = UriFactory.CreateDocumentUri(databaseName, collectionName, id);
            ResourceResponse<Document> document = null;

            try
            {
                document = await client.ReadDocumentAsync(
                    documentsLink,
                    new RequestOptions { PartitionKey = new PartitionKey(partitionKey) });
            }
            catch (DocumentClientException ex)
            {
                if ((int)ex.StatusCode == 404)
                {
                    document = null;
                }
                else if ((int)ex.StatusCode == 429)
                {
                    // If the write is rate limited, wait for twice the recommended wait time specified in the exception
                    int sleepTime = (int)ex.RetryAfter.TotalMilliseconds * 2;

                    // Custom retry logic to keep retrying when the document read is rate limited
                    bool success = false;
                    while (!success && numRetries <= maxRetriesOnDocumentClientExceptions)
                    {
                        // Sleep for twice the recommended amount from the Cosmos DB rate limiting exception
                        Thread.Sleep(sleepTime);

                        try
                        {
                            document = await client.ReadDocumentAsync(documentsLink);
                        }
                        catch (DocumentClientException e)
                        {
                            if ((int)e.StatusCode == 404)
                            {
                                success = true;
                            }
                            else if ((int)e.StatusCode == 429)
                            {
                                sleepTime = (int)e.RetryAfter.TotalMilliseconds * 2;
                                numRetries++;
                            }
                        }
                        catch (Exception exception)
                        {
                            numRetries++;
                        }
                    }
                }
            }

            return document;
        }

        /// <summary>
        /// Deletes a document from the specified Cosmos DB collection and retries when rate limited
        /// </summary>
        /// <param name="client">DocumentClient instance to interact with Azure Cosmos DB</param>
        /// <param name="databaseName">Database name of the collection containing the document to read</param>
        /// <param name="collectionName">Collection name containing the document</param>
        /// <param name="partitionKey">Partition key of the document to delete</param>
        /// <param name="id">Id property of the document to delete</param>
        /// <param name="maxRetriesOnDocumentClientExceptions">Maximum number of retries when rate limited</param>
        /// <returns></returns>
        public static async Task<bool> DeleteDocmentAsync(
            DocumentClient client,
            string databaseName,
            string collectionName,
            string partitionKey,
            string id,
            int maxRetriesOnDocumentClientExceptions)
        {
            int numRetries = 0;
            Uri documentsLink = UriFactory.CreateDocumentUri(databaseName, collectionName, id);
            ResourceResponse<Document> document = null;
            bool success = false;

            try
            {
                document = await client.DeleteDocumentAsync(
                    documentsLink,
                    new RequestOptions { PartitionKey = new PartitionKey(partitionKey) });

                return true;
            }
            catch (DocumentClientException ex)
            {
                if ((int)ex.StatusCode == 404)
                {
                    success = true;
                }
                else if ((int)ex.StatusCode == 429)
                {
                    // If the write is rate limited, wait for twice the recommended wait time specified in the exception
                    int sleepTime = (int)ex.RetryAfter.TotalMilliseconds * 2;

                    // Custom retry logic to keep retrying when the document read is rate limited
                    while (!success && numRetries <= maxRetriesOnDocumentClientExceptions)
                    {
                        // Sleep for twice the recommended amount from the Cosmos DB rate limiting exception
                        Thread.Sleep(sleepTime);

                        try
                        {
                            await client.DeleteDocumentAsync(
                                documentsLink,
                                new RequestOptions { PartitionKey = new PartitionKey(partitionKey) });
                        }
                        catch (DocumentClientException e)
                        {
                            if ((int)e.StatusCode == 404)
                            {
                                success = true;
                            }
                            else if ((int)e.StatusCode == 429)
                            {
                                sleepTime = (int)e.RetryAfter.TotalMilliseconds * 2;
                                numRetries++;
                            }
                        }
                        catch (Exception exception)
                        {
                            numRetries++;
                        }
                    }
                }
            }

            return success;
        }

        /// <summary>
        /// Upserts the specified document in Cosmos DB and retries when rate limited
        /// </summary>
        /// <param name="client">DocumentClient instance to interact with Azure Cosmos DB Service</param>
        /// <param name="databaseName">Database name of the collection containing the document to read</param>
        /// <param name="collectionName">Collection name containing the document</param>
        /// <param name="document">Document to upsert</param>
        /// <param name="maxRetriesOnDocumentClientExceptions">Maximum number of retries when rate limited</param>
        /// <returns></returns>
        public static async Task<ResourceResponse<Document>> UpsertDocumentAsync(
            DocumentClient client,
            string databaseName,
            string collectionName,
            object document,
            int maxRetriesOnDocumentClientExceptions)
        {
            int numRetries = 0;
            Uri documentsFeedLink = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName);

            ResourceResponse<Document> upsertedDocument = null;
            try
            {
                upsertedDocument = await client.UpsertDocumentAsync(documentsFeedLink, document, null, true);
            }
            catch (DocumentClientException ex)
            {
                // Retry when rate limited for as many times as specified
                if ((int)ex.StatusCode == 429)
                {
                    // If the write is rate limited, wait for twice the recommended wait time specified in the exception
                    int sleepTime = (int)ex.RetryAfter.TotalMilliseconds * 2;

                    bool success = false;
                    while (!success && numRetries <= maxRetriesOnDocumentClientExceptions)
                    {
                        // Sleep for twice the recommended amount from the Cosmos DB rate limiting exception
                        Thread.Sleep(sleepTime);

                        try
                        {
                            upsertedDocument = await client.UpsertDocumentAsync(documentsFeedLink, document, null, true);
                            success = true;
                        }
                        catch (DocumentClientException e)
                        {
                            if ((int)e.StatusCode == 429)
                            {
                                sleepTime = (int)e.RetryAfter.TotalMilliseconds * 2;
                            }

                            numRetries++;
                        }
                        catch (Exception exception)
                        {
                            numRetries++;
                        }
                    }
                }
            }

            return upsertedDocument;
        }

        /// <summary>
        /// Creates the specified document in Cosmos DB and retries when rate limited
        /// </summary>
        /// <param name="client">DocumentClient instance to interact with Azure Cosmos DB Service</param>
        /// <param name="databaseName">Database name of the collection containing the document to read</param>
        /// <param name="collectionName">Collection name containing the document</param>
        /// <param name="document">Document to create</param>
        /// <param name="maxRetriesOnDocumentClientExceptions">Maximum number of retries when rate limited</param>
        /// <returns></returns>
        public static async Task<ResourceResponse<Document>> ReplaceDocumentAsync(
            DocumentClient client,
            string databaseName,
            string collectionName,
            string documentId,
            object document,
            RequestOptions requestOptions,
            int maxRetriesOnDocumentClientExceptions)
        {
            int numRetries = 0;
            Uri documentUri = UriFactory.CreateDocumentUri(databaseName, collectionName, documentId);
            ResourceResponse<Document> replacedDocument = null;
            try
            {
                replacedDocument = await client.ReplaceDocumentAsync(documentUri.ToString(), document, requestOptions);
            }
            catch (DocumentClientException ex)
            {
                // Retry when rate limited for as many times as specified
                if ((int)ex.StatusCode == 429)
                {
                    // If the write is rate limited, wait for twice the recommended wait time specified in the exception
                    int sleepTime = (int)ex.RetryAfter.TotalMilliseconds * 2;

                    bool success = false;
                    while (!success && numRetries <= maxRetriesOnDocumentClientExceptions)
                    {
                        // Sleep for twice the recommended amount from the Cosmos DB rate limiting exception
                        Thread.Sleep(sleepTime);

                        try
                        {
                            replacedDocument = await client.ReplaceDocumentAsync(documentUri.ToString(), document, requestOptions);
                            success = true;
                        }
                        catch (DocumentClientException e)
                        {
                            if ((int)e.StatusCode == 429)
                            {
                                sleepTime = (int)e.RetryAfter.TotalMilliseconds * 2;
                            }

                            numRetries++;
                        }
                        catch (Exception exception)
                        {
                            numRetries++;
                        }
                    }
                }
            }

            return replacedDocument;
        }

        /// <summary>
        /// Creates the specified document in Cosmos DB and retries when rate limited
        /// </summary>
        /// <param name="client">DocumentClient instance to interact with Azure Cosmos DB Service</param>
        /// <param name="databaseName">Database name of the collection containing the document to read</param>
        /// <param name="collectionName">Collection name containing the document</param>
        /// <param name="document">Document to create</param>
        /// <param name="maxRetriesOnDocumentClientExceptions">Maximum number of retries when rate limited</param>
        /// <returns></returns>
        public static async Task<ResourceResponse<Document>> CreateDocumentAsync(
            DocumentClient client,
            string databaseName,
            string collectionName,
            object document,
            int maxRetriesOnDocumentClientExceptions)
        {
            int numRetries = 0;
            Uri documentsFeedLink = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName);

            ResourceResponse<Document> createdDocument = null;
            try
            {
                createdDocument = await client.CreateDocumentAsync(documentsFeedLink, document, null, true);
            }
            catch (DocumentClientException ex)
            {
                // Retry when rate limited for as many times as specified
                if ((int)ex.StatusCode == 429)
                {
                    // If the write is rate limited, wait for twice the recommended wait time specified in the exception
                    int sleepTime = (int)ex.RetryAfter.TotalMilliseconds * 2;

                    bool success = false;
                    while (!success && numRetries <= maxRetriesOnDocumentClientExceptions)
                    {
                        // Sleep for twice the recommended amount from the Cosmos DB rate limiting exception
                        Thread.Sleep(sleepTime);

                        try
                        {
                            createdDocument = await client.CreateDocumentAsync(documentsFeedLink, document, null, true);
                            success = true;
                        }
                        catch (DocumentClientException e)
                        {
                            if ((int)e.StatusCode == 429)
                            {
                                sleepTime = (int)e.RetryAfter.TotalMilliseconds * 2;
                            }

                            numRetries++;
                        }
                        catch (Exception exception)
                        {
                            numRetries++;
                        }
                    }
                }
            }

            return createdDocument;
        }
    }
}
