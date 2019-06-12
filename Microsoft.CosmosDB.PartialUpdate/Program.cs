
namespace Microsoft.Azure.CosmosDB.CosmosDBPartialUpdate
{
    using PartialUpdateOptions;
    using Newtonsoft.Json.Linq;

    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents;
    
    using Microsoft.Azure.CosmosDB.PartialUpdate;

    using System;
    using System.Threading.Tasks;

    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                DocumentClient client = CreateDocumentClient();
                string databaseName = "partialUpdateDatabase";
                string collectionName = "test";

                //ExecuteRootLevelUpdate();
                //ExecuteRootLevelArrayUpdate();
                ExecuteNestedObjectUpdate(client, databaseName, collectionName).Wait();
            }
            catch (DocumentClientException ex)
            {

            }
        }

        private static async Task ExecuteRootLevelUpdate(DocumentClient client, string databaseName, string collectionName)
        {
            CosmosDBPartialUpdater partialUpdater = new CosmosDBPartialUpdater(client);

            JObject sampleDocumentToUpdate = GetSampleDocument();
            JObject sampleUpdateDocument = GetSamplePartialUpdateDocument();
            PartialUpdateMergeOptions partialUpdateMergeOptions = CreatePartialUpdateOptions();

            await partialUpdater.ExecuteUpdate(databaseName, collectionName, sampleDocumentToUpdate, sampleUpdateDocument, partialUpdateMergeOptions);
        }

        private static async Task ExecuteRootLevelArrayUpdate(DocumentClient client, string databaseName, string collectionName)
        {
            CosmosDBPartialUpdater partialUpdater = new CosmosDBPartialUpdater(client);

            JObject sampleDocumentToUpdate = GetSampleDocument();
            JObject sampleUpdateDocument = GetSamplePartialUpdateDocumentWithArray();
            PartialUpdateMergeOptions partialUpdateMergeOptions = CreatePartialUpdateOptionsForRootLevelArrayUpdate();

            await partialUpdater.ExecuteUpdate(databaseName, collectionName, sampleDocumentToUpdate, sampleUpdateDocument, partialUpdateMergeOptions);
        }

        private static async Task ExecuteNestedObjectUpdate(DocumentClient client, string databaseName, string collectionName)
        {
            CosmosDBPartialUpdater partialUpdater = new CosmosDBPartialUpdater(client);

            JObject sampleDocumentToUpdate = GetSampleDocument();
            JObject sampleUpdateDocument = GetSamplePartialUpdateDocumentWithNestedObject();
            PartialUpdateMergeOptions partialUpdateMergeOptions = CreatePartialUpdateOptionsForNestedObjectUpdate();

            //await partialUpdater.ExecutePartialUpdate(databaseName, collectionName, "123", "123", sampleUpdateDocument, partialUpdateMergeOptions);
            await partialUpdater.ExecuteUpdate(databaseName, collectionName, sampleDocumentToUpdate, sampleUpdateDocument, partialUpdateMergeOptions);
            Console.ReadLine();
        }

        private static JObject GetSampleDocument()
        {
            string sampleDocumentJSON = "{\"id\":\"123\",\"firstName\":\"Abinav\",\"lastName\":\"Ramesh\",\"dateOfBirth\":\"23 - July - 1988\",\"employer\":\"AlaskaAirlines\",\"previousEmployer\":\"PROS,inc ? \",\"citiesLivedIn\":[\"Dubai\",\"AnnArbor\",\"Ithaca\",\"Houston\",\"Seattle\"],\"previousJobs\":[{\"id\":\"1\",\"bookingClasses\":[{\"id\":\"123456789\",\"totalSeatsAvailable\":75,\"totalBookings\":30,\"grouBookings\":12,\"compartmentCodes\":[{\"id\":\"1234567890\",\"name\":\"Z\",\"class\":\"Business\"}]}]},{\"id\":\"2\",\"title\":\"SoftwareEngineer1\",\"employer\":\"PROS,INC\",\"location\":\"Houston\",\"managers\":[\"Aristotle\",\"Bartholomew\",\"Columbus\",\"Alexander\"]},{\"id\":\"3\",\"title\":\"SoftwareEngineer1\",\"employer\":\"MicrosoftCorporation\",\"location\":\"Seattle\",\"managers\":[\"Aristotle\",\"Bartholomew\",\"Columbus\",\"Alexander\",\"Aristotle\"]},{\"id\":\"4\",\"title\":\"SoftwareEngineer2\",\"employer\":\"MicrosoftCorporation\",\"location\":\"Seattle\",\"managers\":[\"Gates\",\"Bartholomey\"],\"previousEmployer\":\"PROS\"}]}";
            //string sampleDocument = "{\"id\":\"1\",\"previousJobs\":[{\"id\":\"2\",\"title\":\"SoftwareEngineer1\",\"employer\":\"PROS,INC\",\"location\":\"Houston\",\"managers\":[\"Aristotle\",\"Bartholomew\",\"Columbus\",\"Alexander\",\"Gates\",\"Jobbs\"]}]}";
            JObject objectToReturn = JObject.Parse(sampleDocumentJSON);

            return objectToReturn;
        }

        private static JObject GetSamplePartialUpdateDocument()
        {
            string samplePartialUpdateDocumentString = "{\"employer\":\"PROS INC?\"}";

            JObject samplePartialUpdateDocument = JObject.Parse(samplePartialUpdateDocumentString);
            return samplePartialUpdateDocument;
        }

        private static JObject GetSamplePartialUpdateDocumentWithArray()
        {
            string samplePartialUpdateDocumentString = "{\"managers\": [\"Aristotle\",\"Gates\",\"Jobbs\"]}";

            JObject samplePartialUpdateDocument = JObject.Parse(samplePartialUpdateDocumentString);
            return samplePartialUpdateDocument;
        }

        private static JObject GetSamplePartialUpdateDocumentWithNestedObject()
        {
            string samplePartialUpdateDocumentString = "{\"title\":\"Software Engineer 2\",\"employer\":\"PROS,INC\",\"location\":\"Houston\",\"managers\":[\"Aristotle\",\"Bartholomew\",\"Columbus\",\"Alexander\"]}";

            JObject samplePartialUpdateDocument = JObject.Parse(samplePartialUpdateDocumentString);
            return samplePartialUpdateDocument;
        }

        private static PartialUpdateMergeOptions CreatePartialUpdateOptions()
        {
            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.UNION;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.UPDATE;
            partialUpdateMergeOptions.objectFilteringPropertyName = "id";
            partialUpdateMergeOptions.objectFilteringPropertyValue = "2";

            return partialUpdateMergeOptions;
        }

        private static PartialUpdateMergeOptions CreatePartialUpdateOptionsForRootLevelArrayUpdate()
        {
            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.CONCAT;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.UPDATE;
            partialUpdateMergeOptions.objectFilteringPropertyName = "id";
            partialUpdateMergeOptions.objectFilteringPropertyValue = "3";

            return partialUpdateMergeOptions;
        }

        private static PartialUpdateMergeOptions CreatePartialUpdateOptionsForNestedObjectUpdate()
        {
            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.REPLACE;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.REPLACE;
            partialUpdateMergeOptions.objectFilteringPropertyName = "id";
            partialUpdateMergeOptions.objectFilteringPropertyValue = "3";

            return partialUpdateMergeOptions;
        }

        /// <summary>
        /// Creates an instance of the DocumentClient to interact with the Azure Cosmos DB service
        /// </summary>
        /// <returns></returns>
        private static DocumentClient CreateDocumentClient()
        {
            ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            };

            DocumentClient documentClient = null;
            try
            {
                string cosmosDBEndpointUri = "https://abinav-alaska-partial-update.documents.azure.com:443/";
                string accountKey = "dkhC7gfV4h4BbrsQDmQNKpfgVnNRmIMs2tLbqctrAgeLijBwwbeuVkDQfOXTK5adLeMw0h6iRM3pmmlVtT8Hfw==";
                documentClient = new DocumentClient(new Uri(cosmosDBEndpointUri), accountKey, ConnectionPolicy);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception while creating DocumentClient. Original  exception message was: {0}", ex.Message);
            }

            return documentClient;
        }
    }
}
