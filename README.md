<img src="https://raw.githubusercontent.com/dennyglee/azure-cosmosdb-spark/master/docs/images/azure-cosmos-db-icon.png" width="75">  &nbsp; Azure Cosmos DB Partial Updates Helper for .NET
==========================================

## Consuming the Microsoft Azure Cosmos DB Partial Updates .NET/.NET Core library

The purpose of this project is to make the development effort for partial updates simpler by abstracting away from the user the operations to fetch the document(s) to be updated, deserializing the documents, executing the partial update and then pushing the documents back to the Azure Cosmos DB container. This project includes samples, tests and documentation for consuming the Partial Updates helper library. 

## Partial Updates API

We provide two overloads of the partial update API - one which identifies the document to be updated using the partition key and id and another which retrieves the document(s) to be updated given a query string.

* With the partition key and id of the document to update
```csharp
        Task<ResourceResponse<Document>> ExecutePartialUpdate(
            string databaseName,
            string collectionName,
            string partitionKey,
            string id,
            JObject partialUpdateDocument,
            PartialUpdateMergeOptions partialUpdateMergeOptions = null);

```

* With the query string to determine the document(s) to update
```csharp
        Task<List<ResourceResponse<Document>>> ExecutePartialUpdate(
            string databaseName,
            string collectionName,
            string queryText,
            JObject partialUpdateDocument,
            string partitionKey = null,
            PartialUpdateMergeOptions partialUpdateMergeOptions = null);

```

### PartialUpdateMergeOptions

PartialUpdateMergeOptions can be used to specify the nature of the partial updates to be executed. In particular, for arrays and objects as well as specifications for handling null values.

* *ArrayMergeOptions* : UNION (default), MERGE, CONCAT or REPLACE
* *NullValueMergeOptions* : IGNORE (default) or MERGE
* *ObjectMergeOptions* : UPDATE (default) or MERGE.

### Examples of Partial updates

* Partial update given the partition key and id of the document to be updated

```csharp
// Initialize the DocumentClient
DocumentClient client = new DocumentClient(
	new Uri(this.CosmosDBAccountEndpoint),
	this.CosmosDBAccountKey,
	new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp });
	
// Initialize PartialUpdateMergeOptions (Note: This is only needed if different from the default. Below are the default values)
PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.UNION;
partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.UPDATE;

// Assign the JObject containing only the fields to be updated
JObject sampleUpdateDocumentWithOnePropertyToUpdate = JObject.Parse("{\"employer\":\"Some Other Company\"}");

// Execute the partial update and retrieve the resulting document
ResourceResponse<Document> updatedDocument = await partialUpdater.ExecutePartialUpdate(
	this.DatabaseName,
	this.CollectionName,
	"123",
	"123",
	sampleUpdateDocumentWithOnePropertyToUpdate,
	partialUpdateMergeOptions);
```

* Partial update given the query to identify the document(s) to be updated

```csharp
// Initialize the DocumentClient
DocumentClient client = new DocumentClient(
	new Uri(this.CosmosDBAccountEndpoint),
	this.CosmosDBAccountKey,
	new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp });
	
// Initialize PartialUpdateMergeOptions (Note: This is only needed if different from the default. Below are the default values)
PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.UNION;
partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.UPDATE;

// Assign the JObject containing only the fields to be updated
JObject sampleUpdateDocumentWithOnePropertyToUpdate = JObject.Parse("{\"employer\":\"Some Other Company\"}");

// Execute the partial update and retrieve the resulting document
List<ResourceResponse<Document>> updatedDocumentList = await partialUpdater.ExecutePartialUpdate(
	this.DatabaseName,
	this.CollectionName,
	"select * from c where c.id = '123'",
	sampleUpdateDocumentWithOnePropertyToUpdate,
	null, // Optional partition key value, not needed if already specified in the query string
	partialUpdateMergeOptions);
```

Tests with additional details for partial updates of nested objects and arrays can be found [here](https://github.com/abinav2307/azure-cosmosdb-partial-updates/blob/master/Microsoft.CosmosDB.PartialUpdates.UnitTests/PartialUpdateTests.cs)
