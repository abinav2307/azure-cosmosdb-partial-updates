using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Azure.CosmosDB.PartialUpdate;
using Microsoft.Azure.CosmosDB.CosmosDBPartialUpdate;
using PartialUpdateOptions;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace Microsoft.Azure.PartialUpdates.UnitTests
{
    [TestClass]
    public class PartialUpdateTests
    {
        private DocumentClient DocumentClient = null;
        private DocumentCollection Collection = null;
        private string DatabaseName = null;
        private string CollectionName = null;
        private int MaxRetriesOnDocumentClienException = 10;

        [TestInitialize]
        public void TestSuiteSetUp()
        {
            this.DocumentClient = new DocumentClient(
                new Uri(ConfigurationManager.AppSettings["CosmosDBAccountEndpoint"]),
                ConfigurationManager.AppSettings["CosmosDBAccountKey"],
                new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp });

            this.DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
            this.CollectionName = ConfigurationManager.AppSettings["CollectionName"];

            this.Collection = RecreateCollectionAsync(
                DocumentClient,
                ConfigurationManager.AppSettings["DatabaseName"],
                ConfigurationManager.AppSettings["CollectionName"],
                true).Result;
        }

        private async Task CreateDocumentAsync(JObject documentToWrite)
        {
            await CosmosDBHelper.CreateDocumentAsync(
                this.DocumentClient, 
                this.DatabaseName, 
                this.CollectionName, 
                documentToWrite, 
                this.MaxRetriesOnDocumentClienException);
        }

        /// <summary>
        /// Test to verify a single root level property in the Document is successfully updated when running a partial update
        /// </summary>
        [TestMethod]
        [Owner("abinav2307")]
        public async Task TestRootLevelPartialUpdateWithSingleField()
        {
            await this.CreateDocumentAsync(GetSampleDocument());

            ResourceResponse<Document> originalDocumentPriorToPartialUpdate = await CosmosDBHelper.ReadDocmentAsync(
                this.DocumentClient, 
                this.DatabaseName, 
                this.CollectionName, 
                "123", 
                "123",
                this.MaxRetriesOnDocumentClienException);

            Assert.IsNotNull(originalDocumentPriorToPartialUpdate);

            // Assert the value of the field to be updated, prior to the update
            Assert.AreEqual(originalDocumentPriorToPartialUpdate.Resource.GetPropertyValue<string>("employer"), "Some Company");

            JObject sampleUpdateDocumentWithOnePropertyToUpdate = JObject.Parse("{\"employer\":\"Some Other Company\"}");

            PartialUpdateMergeOptions partialUpdateMergeOptions = CreatePartialUpdateOptionsForRootLevelPartialUpdate();

            CosmosDBPartialUpdater partialUpdater = new CosmosDBPartialUpdater(this.DocumentClient);

            ResourceResponse<Document> updatedDocument = await partialUpdater.ExecutePartialUpdate(
                this.DatabaseName, 
                this.CollectionName, 
                "123", 
                "123", 
                sampleUpdateDocumentWithOnePropertyToUpdate, 
                partialUpdateMergeOptions);

            JObject documentPostUpdate = RemoveMetadataFieldsFromCosmosDBResponse(updatedDocument);

            // Assert the value of updated field
            Assert.AreEqual(documentPostUpdate.ToString(), GetResultForRootLevelPartialUpdateWithSingleField().ToString());
        }

        /// <summary>
        /// Test to verify a single root level property in the Document is successfully updated 
        /// when running a partial update using the query overload
        /// </summary>
        [TestMethod]
        [Owner("abinav2307")]
        public async Task TestRootLevelPartialUpdateWithSingleFieldUsingQueryOverload()
        {
            await this.CreateDocumentAsync(GetSampleDocument());

            ResourceResponse<Document> originalDocumentPriorToPartialUpdate = await CosmosDBHelper.ReadDocmentAsync(
                this.DocumentClient,
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                this.MaxRetriesOnDocumentClienException);

            Assert.IsNotNull(originalDocumentPriorToPartialUpdate);

            // Assert the value of the field to be updated, prior to the update
            Assert.AreEqual(originalDocumentPriorToPartialUpdate.Resource.GetPropertyValue<string>("employer"), "Some Company");

            JObject sampleUpdateDocumentWithOnePropertyToUpdate = JObject.Parse("{\"employer\":\"Some Other Company\"}");

            PartialUpdateMergeOptions partialUpdateMergeOptions = CreatePartialUpdateOptionsForRootLevelPartialUpdate();

            CosmosDBPartialUpdater partialUpdater = new CosmosDBPartialUpdater(this.DocumentClient);

            List<ResourceResponse<Document>> updatedDocumentList = await partialUpdater.ExecutePartialUpdate(
                this.DatabaseName,
                this.CollectionName,
                "select * from c where c.id = '123'",
                sampleUpdateDocumentWithOnePropertyToUpdate,
                null,
                partialUpdateMergeOptions);

            Assert.AreEqual(updatedDocumentList.Count, 1);

            JObject documentPostUpdate = RemoveMetadataFieldsFromCosmosDBResponse(updatedDocumentList[0]);

            // Assert the value of updated field
            Assert.AreEqual(documentPostUpdate.ToString(), GetResultForRootLevelPartialUpdateWithSingleField().ToString());
        }

        /// <summary>
        /// Test to verify multiple root level properties in the Document are successfully updated when running a partial update
        /// </summary>
        [TestMethod]
        [Owner("abinav2307")]
        public async Task TestRootLevelPartialUpdateWithMultipleFields()
        {
            await this.CreateDocumentAsync(GetSampleDocument());

            await this.CreateDocumentAsync(GetSampleDocument());

            ResourceResponse<Document> originalDocumentPriorToPartialUpdate = await CosmosDBHelper.ReadDocmentAsync(
                this.DocumentClient,
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                this.MaxRetriesOnDocumentClienException);

            Assert.IsNotNull(originalDocumentPriorToPartialUpdate);

            // Swapping the values of the 'employer' and 'previousEmployer' for the partial update
            JObject sampleUpdateDocumentWithOnePropertyToUpdate = JObject.Parse("{\"employer\":\"Some Other Company\", \"previousEmployer\": \"Some Company\"}");

            PartialUpdateMergeOptions partialUpdateMergeOptions = CreatePartialUpdateOptionsForRootLevelPartialUpdate();

            CosmosDBPartialUpdater partialUpdater = new CosmosDBPartialUpdater(this.DocumentClient);

            ResourceResponse<Document> updatedDocument = await partialUpdater.ExecutePartialUpdate(
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                sampleUpdateDocumentWithOnePropertyToUpdate,
                partialUpdateMergeOptions);

            JObject documentPostUpdate = RemoveMetadataFieldsFromCosmosDBResponse(updatedDocument);

            // Assert the value of updated field
            Assert.AreEqual(documentPostUpdate.ToString(), GetResultForRootLevelPartialUpdateWithMultipleFields().ToString());
        }

        /// <summary>
        /// Test to verify an array is successfully updated with UNION ArrayMergeOptions
        /// </summary>
        [TestMethod]
        [Owner("abinav2307")]
        public async Task TestSuccessfulUpdateOfArrayUsingUnionMergeOption()
        {
            await this.CreateDocumentAsync(GetSampleDocument());

            ResourceResponse<Document> originalDocumentPriorToPartialUpdate = await CosmosDBHelper.ReadDocmentAsync(
                this.DocumentClient,
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                this.MaxRetriesOnDocumentClienException);

            Assert.IsNotNull(originalDocumentPriorToPartialUpdate);

            JObject sampleUpdateDocumentWithOnePropertyToUpdate = JObject.Parse("{\"managers\": [\"Aristotle\",\"Gates\",\"Jobbs\"]}");

            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.UNION;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.UPDATE;
            partialUpdateMergeOptions.objectFilteringPropertyName = "id";
            partialUpdateMergeOptions.objectFilteringPropertyValue = "1";

            CosmosDBPartialUpdater partialUpdater = new CosmosDBPartialUpdater(this.DocumentClient);

            ResourceResponse<Document> updatedDocument = await partialUpdater.ExecutePartialUpdate(
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                sampleUpdateDocumentWithOnePropertyToUpdate,
                partialUpdateMergeOptions);

            JObject documentPostUpdate = RemoveMetadataFieldsFromCosmosDBResponse(updatedDocument);

            // Assert the value of updated field
            Assert.AreEqual(documentPostUpdate.ToString(), GetResultForSuccessfulUpdateOfArrayUsingUnionMergeOption().ToString());
        }

        /// <summary>
        /// Test to verify an array is successfully updated with CONCAT ArrayMergeOptions
        /// </summary>
        [TestMethod]
        [Owner("abinav2307")]
        public async Task TestSuccessfulUpdateOfArrayUsingConcatMergeOption()
        {
            await this.CreateDocumentAsync(GetSampleDocument());

            ResourceResponse<Document> originalDocumentPriorToPartialUpdate = await CosmosDBHelper.ReadDocmentAsync(
                this.DocumentClient,
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                this.MaxRetriesOnDocumentClienException);

            Assert.IsNotNull(originalDocumentPriorToPartialUpdate);

            JObject sampleUpdateDocumentWithOnePropertyToUpdate = JObject.Parse("{\"managers\": [\"Aristotle\",\"Gates\",\"Jobbs\"]}");

            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.CONCAT;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.UPDATE;
            partialUpdateMergeOptions.objectFilteringPropertyName = "id";
            partialUpdateMergeOptions.objectFilteringPropertyValue = "1";

            CosmosDBPartialUpdater partialUpdater = new CosmosDBPartialUpdater(this.DocumentClient);

            ResourceResponse<Document> updatedDocument = await partialUpdater.ExecutePartialUpdate(
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                sampleUpdateDocumentWithOnePropertyToUpdate,
                partialUpdateMergeOptions);

            JObject documentPostUpdate = RemoveMetadataFieldsFromCosmosDBResponse(updatedDocument);

            // Assert the value of updated field
            Assert.AreEqual(documentPostUpdate.ToString(), GetResultForSuccessfulUpdateOfArrayUsingConcatMergeOption().ToString());
        }

        /// <summary>
        /// Test to verify an array is successfully updated with REPLACE ArrayMergeOptions
        /// </summary>
        [TestMethod]
        [Owner("abinav2307")]
        public async Task TestSuccessfulUpdateOfArrayUsingReplaceMergeOption()
        {
            await this.CreateDocumentAsync(GetSampleDocument());

            ResourceResponse<Document> originalDocumentPriorToPartialUpdate = await CosmosDBHelper.ReadDocmentAsync(
                this.DocumentClient,
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                this.MaxRetriesOnDocumentClienException);

            Assert.IsNotNull(originalDocumentPriorToPartialUpdate);

            JObject sampleUpdateDocumentWithOnePropertyToUpdate = JObject.Parse("{\"managers\": [\"Aristotle\",\"Gates\",\"Jobbs\"]}");

            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.REPLACE;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.UPDATE;
            partialUpdateMergeOptions.objectFilteringPropertyName = "id";
            partialUpdateMergeOptions.objectFilteringPropertyValue = "1";

            CosmosDBPartialUpdater partialUpdater = new CosmosDBPartialUpdater(this.DocumentClient);

            ResourceResponse<Document> updatedDocument = await partialUpdater.ExecutePartialUpdate(
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                sampleUpdateDocumentWithOnePropertyToUpdate,
                partialUpdateMergeOptions);

            JObject documentPostUpdate = RemoveMetadataFieldsFromCosmosDBResponse(updatedDocument);

            // Assert the value of updated field
            Assert.AreEqual(documentPostUpdate.ToString(), GetResultForSuccessfulUpdateOfArrayUsingReplaceMergeOption().ToString());
        }

        /// <summary>
        /// Test to verify an array is successfully updated with MERGE ArrayMergeOptions
        /// </summary>
        [TestMethod]
        [Owner("abinav2307")]
        public async Task TestSuccessfulUpdateOfArrayUsingMerge_MergeOption()
        {
            await this.CreateDocumentAsync(GetSampleDocument());

            ResourceResponse<Document> originalDocumentPriorToPartialUpdate = await CosmosDBHelper.ReadDocmentAsync(
                this.DocumentClient,
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                this.MaxRetriesOnDocumentClienException);

            Assert.IsNotNull(originalDocumentPriorToPartialUpdate);

            JObject sampleUpdateDocumentWithOnePropertyToUpdate = JObject.Parse("{\"managers\": [\"Aristotle\",\"Gates\",\"Jobbs\"]}");

            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.MERGE;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.UPDATE;
            partialUpdateMergeOptions.objectFilteringPropertyName = "id";
            partialUpdateMergeOptions.objectFilteringPropertyValue = "1";

            CosmosDBPartialUpdater partialUpdater = new CosmosDBPartialUpdater(this.DocumentClient);

            ResourceResponse<Document> updatedDocument = await partialUpdater.ExecutePartialUpdate(
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                sampleUpdateDocumentWithOnePropertyToUpdate,
                partialUpdateMergeOptions);

            JObject documentPostUpdate = RemoveMetadataFieldsFromCosmosDBResponse(updatedDocument);

            // Assert the value of updated field
            Assert.AreEqual(documentPostUpdate.ToString(), GetResultForSuccessfulUpdateOfArrayUsingMerge_MergeOption().ToString());
        }

        /// <summary>
        /// Test to verify a nested object is successfully updated with REPLACE ObjectMergeOptions
        /// </summary>
        [TestMethod]
        [Owner("abinav2307")]
        public async Task TestSuccessfulUpdateOfNestedObjectUsingReplaceMergeOption()
        {
            await this.CreateDocumentAsync(GetSampleDocument());

            ResourceResponse<Document> originalDocumentPriorToPartialUpdate = await CosmosDBHelper.ReadDocmentAsync(
                this.DocumentClient,
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                this.MaxRetriesOnDocumentClienException);

            Assert.IsNotNull(originalDocumentPriorToPartialUpdate);

            JObject sampleUpdateDocumentWithObjectUpdate =
                JObject.Parse("{\"title\":\"Software Engineer 2\",\"employer\":\"One Last Company Again\",\"location\":\"Houston\",\"managers\":[\"Aristotle\",\"Bartholomew\",\"Columbus\",\"Alexander\"]}");

            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.MERGE;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.REPLACE;
            partialUpdateMergeOptions.objectFilteringPropertyName = "id";
            partialUpdateMergeOptions.objectFilteringPropertyValue = "4";

            CosmosDBPartialUpdater partialUpdater = new CosmosDBPartialUpdater(this.DocumentClient);

            ResourceResponse<Document> updatedDocument = await partialUpdater.ExecutePartialUpdate(
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                sampleUpdateDocumentWithObjectUpdate,
                partialUpdateMergeOptions);

            JObject documentPostUpdate = RemoveMetadataFieldsFromCosmosDBResponse(updatedDocument);

            // Assert the value of updated field
            Assert.AreEqual(documentPostUpdate.ToString(), GetResultForNestedObjectUsingReplaceMergeOption().ToString());
        }

        /// <summary>
        /// Test to verify a nested object is successfully updated with UPDATE ObjectMergeOptions
        /// </summary>
        [TestMethod]
        [Owner("abinav2307")]
        public async Task TestSuccessfulUpdateOfNestedObjectUsingUpdateMergeOption()
        {
            await this.CreateDocumentAsync(GetSampleDocument());

            ResourceResponse<Document> originalDocumentPriorToPartialUpdate = await CosmosDBHelper.ReadDocmentAsync(
                this.DocumentClient,
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                this.MaxRetriesOnDocumentClienException);

            Assert.IsNotNull(originalDocumentPriorToPartialUpdate);

            JObject sampleUpdateDocumentWithObjectUpdate =
                JObject.Parse("{\"title\":\"Software Engineer 2\",\"employer\":\"One Last Company Again\",\"location\":\"Houston\",\"managers\":[\"Aristotle\",\"Bartholomew\",\"Columbus\",\"Alexander\"]}");

            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.MERGE;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.UPDATE;
            partialUpdateMergeOptions.objectFilteringPropertyName = "id";
            partialUpdateMergeOptions.objectFilteringPropertyValue = "4";

            CosmosDBPartialUpdater partialUpdater = new CosmosDBPartialUpdater(this.DocumentClient);

            ResourceResponse<Document> updatedDocument = await partialUpdater.ExecutePartialUpdate(
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                sampleUpdateDocumentWithObjectUpdate,
                partialUpdateMergeOptions);

            JObject documentPostUpdate = RemoveMetadataFieldsFromCosmosDBResponse(updatedDocument);

            // Assert the value of updated field
            Assert.AreEqual(documentPostUpdate.ToString(), GetResultForNestedObjectUsingUpdateMergeOption().ToString());
        }

        /// <summary>
        /// Test to verify a nested object is successfully updated with UPDATE ObjectMergeOptions
        /// </summary>
        [TestMethod]
        [Owner("abinav2307")]
        public async Task TestSuccessfulAdditionOfObjectToAnArrayOfObjectsUsingArrayUpdateMergeOption()
        {
            await this.CreateDocumentAsync(GetSampleDocument());

            ResourceResponse<Document> originalDocumentPriorToPartialUpdate = await CosmosDBHelper.ReadDocmentAsync(
                this.DocumentClient,
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                this.MaxRetriesOnDocumentClienException);

            Assert.IsNotNull(originalDocumentPriorToPartialUpdate);

            JObject sampleUpdateDocumentWithObjectUpdate =
                JObject.Parse("{\"previousJobs\":[{\"id\":\"5\",\"title\":\"Software Engineer 2\",\"employer\":\"Still Another Company\",\"location\":\"Helsinki\",\"managers\":[\"Kobe\",\"Shaq\",\"Kareem\",\"Magic\"]}]}");

            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.MERGE;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.UPDATE;
            partialUpdateMergeOptions.objectFilteringPropertyName = "id";
            partialUpdateMergeOptions.objectFilteringPropertyValue = "123";

            CosmosDBPartialUpdater partialUpdater = new CosmosDBPartialUpdater(this.DocumentClient);

            ResourceResponse<Document> updatedDocument = await partialUpdater.ExecutePartialUpdate(
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                sampleUpdateDocumentWithObjectUpdate,
                partialUpdateMergeOptions);

            JObject documentPostUpdate = RemoveMetadataFieldsFromCosmosDBResponse(updatedDocument);

            // Assert the value of updated field
            Assert.AreEqual(documentPostUpdate.ToString(), GetResultForSuccessfulAdditionOfObjectToAnArrayOfObjectsUsingArrayUpdateMergeOption().ToString());
        }

        /// <summary>
        /// Test to verify a nested object is successfully updated with UPDATE ObjectMergeOptions
        /// </summary>
        [TestMethod]
        [Owner("abinav2307")]
        public async Task TestFailureInvalidObjectIdInMergeOptions()
        {
            await this.CreateDocumentAsync(GetSampleDocument());

            ResourceResponse<Document> originalDocumentPriorToPartialUpdate = await CosmosDBHelper.ReadDocmentAsync(
                this.DocumentClient,
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                this.MaxRetriesOnDocumentClienException);

            Assert.IsNotNull(originalDocumentPriorToPartialUpdate);

            JObject sampleUpdateDocumentWithOnePropertyToUpdate = JObject.Parse("{\"managers\": [\"Aristotle\",\"Gates\",\"Jobbs\"]}");

            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.MERGE;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.UPDATE;
            partialUpdateMergeOptions.objectFilteringPropertyName = "id";
            partialUpdateMergeOptions.objectFilteringPropertyValue = "12";

            CosmosDBPartialUpdater partialUpdater = new CosmosDBPartialUpdater(this.DocumentClient);

            try
            {
                ResourceResponse<Document> updatedDocument = await partialUpdater.ExecutePartialUpdate(
                this.DatabaseName,
                this.CollectionName,
                "123",
                "123",
                sampleUpdateDocumentWithOnePropertyToUpdate,
                partialUpdateMergeOptions);
            }
            catch (ArgumentException ex)
            {
                Assert.AreEqual(ex.Message, "Object with filter: id and value: 12 does not exist.");
            }
            catch (Exception e)
            {
                Assert.Fail("Should not have thrown an exception other than ArgumentException");
            }
        }

        private static JObject RemoveMetadataFieldsFromCosmosDBResponse(ResourceResponse<Document> updatedDocument)
        {
            JObject documentPostUpdate = JObject.Parse(updatedDocument.Resource.ToString());
            documentPostUpdate.Remove("_rid");
            documentPostUpdate.Remove("_self");
            documentPostUpdate.Remove("_attachments");
            documentPostUpdate.Remove("_ts");
            documentPostUpdate.Remove("_etag");

            return documentPostUpdate;
        }

        private static JObject GetSampleDocument()
        {
            return JObject.Parse(
                @"{
                    ""id"":""123"",
                    ""firstName"":""Abinav"",
                    ""lastName"":""Ramesh"",
                    ""dateOfBirth"":""23 - July - 1988"",
                    ""employer"":""Some Company"",
                    ""previousEmployer"":""Some Other Company"",
                    ""citiesLivedIn"": [
                        ""Dubai"",
                        ""AnnArbor"",
                        ""Ithaca"",
                        ""Houston"",
                        ""Seattle""],
                    ""previousJobs"":[
                        {
                            ""id"":""1"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Company"",
                            ""location"":""Houston"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""2"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Other Company"",
                            ""location"":""Ann Arbor"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""3"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Yet Another Company"",
                            ""location"":""Ithaca"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander"",
                                ""Aristotle""]
                        },
                        {
                            ""id"":""4"",
                            ""title"":""Software Engineer"",
                            ""employer"":""One Last Company"",
                            ""location"":""Seattle"",
                            ""managers"":[
                                ""Gates"",
                                ""Bartholomew""]
                        }]
                  }");
        }

        private static JObject GetResultForRootLevelPartialUpdateWithSingleField()
        {
            return JObject.Parse(
                @"{
                    ""id"":""123"",
                    ""firstName"":""Abinav"",
                    ""lastName"":""Ramesh"",
                    ""dateOfBirth"":""23 - July - 1988"",
                    ""employer"":""Some Other Company"",
                    ""previousEmployer"":""Some Other Company"",
                    ""citiesLivedIn"": [
                        ""Dubai"",
                        ""AnnArbor"",
                        ""Ithaca"",
                        ""Houston"",
                        ""Seattle""],
                    ""previousJobs"":[
                        {
                            ""id"":""1"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Company"",
                            ""location"":""Houston"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""2"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Other Company"",
                            ""location"":""Ann Arbor"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""3"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Yet Another Company"",
                            ""location"":""Ithaca"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander"",
                                ""Aristotle""]
                        },
                        {
                            ""id"":""4"",
                            ""title"":""Software Engineer"",
                            ""employer"":""One Last Company"",
                            ""location"":""Seattle"",
                            ""managers"":[
                                ""Gates"",
                                ""Bartholomew""]
                        }]
                  }");
        }

        private static JObject GetResultForRootLevelPartialUpdateWithMultipleFields()
        {
            return JObject.Parse(
                @"{
                    ""id"":""123"",
                    ""firstName"":""Abinav"",
                    ""lastName"":""Ramesh"",
                    ""dateOfBirth"":""23 - July - 1988"",
                    ""employer"":""Some Other Company"",
                    ""previousEmployer"":""Some Company"",
                    ""citiesLivedIn"": [
                        ""Dubai"",
                        ""AnnArbor"",
                        ""Ithaca"",
                        ""Houston"",
                        ""Seattle""],
                    ""previousJobs"":[
                        {
                            ""id"":""1"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Company"",
                            ""location"":""Houston"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""2"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Other Company"",
                            ""location"":""Ann Arbor"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""3"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Yet Another Company"",
                            ""location"":""Ithaca"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander"",
                                ""Aristotle""]
                        },
                        {
                            ""id"":""4"",
                            ""title"":""Software Engineer"",
                            ""employer"":""One Last Company"",
                            ""location"":""Seattle"",
                            ""managers"":[
                                ""Gates"",
                                ""Bartholomew""]
                        }]
                  }");
        }

        private static JObject GetResultForSuccessfulUpdateOfArrayUsingUnionMergeOption()
        {
            return JObject.Parse(
                @"{
                    ""id"":""123"",
                    ""firstName"":""Abinav"",
                    ""lastName"":""Ramesh"",
                    ""dateOfBirth"":""23 - July - 1988"",
                    ""employer"":""Some Company"",
                    ""previousEmployer"":""Some Other Company"",
                    ""citiesLivedIn"": [
                        ""Dubai"",
                        ""AnnArbor"",
                        ""Ithaca"",
                        ""Houston"",
                        ""Seattle""],
                    ""previousJobs"":[
                        {
                            ""id"":""1"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Company"",
                            ""location"":""Houston"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander"",
                                ""Gates"",
                                ""Jobbs""]
                        },
                        {
                            ""id"":""2"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Other Company"",
                            ""location"":""Ann Arbor"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""3"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Yet Another Company"",
                            ""location"":""Ithaca"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander"",
                                ""Aristotle""]
                        },
                        {
                            ""id"":""4"",
                            ""title"":""Software Engineer"",
                            ""employer"":""One Last Company"",
                            ""location"":""Seattle"",
                            ""managers"":[
                                ""Gates"",
                                ""Bartholomew""]
                        }]
                  }");
        }

        private static JObject GetResultForSuccessfulUpdateOfArrayUsingConcatMergeOption()
        {
            return JObject.Parse(
                @"{
                    ""id"":""123"",
                    ""firstName"":""Abinav"",
                    ""lastName"":""Ramesh"",
                    ""dateOfBirth"":""23 - July - 1988"",
                    ""employer"":""Some Company"",
                    ""previousEmployer"":""Some Other Company"",
                    ""citiesLivedIn"": [
                        ""Dubai"",
                        ""AnnArbor"",
                        ""Ithaca"",
                        ""Houston"",
                        ""Seattle""],
                    ""previousJobs"":[
                        {
                            ""id"":""1"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Company"",
                            ""location"":""Houston"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander"",
                                ""Aristotle"",
                                ""Gates"",
                                ""Jobbs""]
                        },
                        {
                            ""id"":""2"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Other Company"",
                            ""location"":""Ann Arbor"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""3"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Yet Another Company"",
                            ""location"":""Ithaca"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander"",
                                ""Aristotle""]
                        },
                        {
                            ""id"":""4"",
                            ""title"":""Software Engineer"",
                            ""employer"":""One Last Company"",
                            ""location"":""Seattle"",
                            ""managers"":[
                                ""Gates"",
                                ""Bartholomew""]
                        }]
                  }");
        }

        private static JObject GetResultForSuccessfulUpdateOfArrayUsingReplaceMergeOption()
        {
            return JObject.Parse(
                @"{
                    ""id"":""123"",
                    ""firstName"":""Abinav"",
                    ""lastName"":""Ramesh"",
                    ""dateOfBirth"":""23 - July - 1988"",
                    ""employer"":""Some Company"",
                    ""previousEmployer"":""Some Other Company"",
                    ""citiesLivedIn"": [
                        ""Dubai"",
                        ""AnnArbor"",
                        ""Ithaca"",
                        ""Houston"",
                        ""Seattle""],
                    ""previousJobs"":[
                        {
                            ""id"":""1"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Company"",
                            ""location"":""Houston"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Gates"",
                                ""Jobbs""]
                        },
                        {
                            ""id"":""2"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Other Company"",
                            ""location"":""Ann Arbor"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""3"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Yet Another Company"",
                            ""location"":""Ithaca"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander"",
                                ""Aristotle""]
                        },
                        {
                            ""id"":""4"",
                            ""title"":""Software Engineer"",
                            ""employer"":""One Last Company"",
                            ""location"":""Seattle"",
                            ""managers"":[
                                ""Gates"",
                                ""Bartholomew""]
                        }]
                  }");
        }

        private static JObject GetResultForSuccessfulUpdateOfArrayUsingMerge_MergeOption()
        {
            return JObject.Parse(
                @"{
                    ""id"":""123"",
                    ""firstName"":""Abinav"",
                    ""lastName"":""Ramesh"",
                    ""dateOfBirth"":""23 - July - 1988"",
                    ""employer"":""Some Company"",
                    ""previousEmployer"":""Some Other Company"",
                    ""citiesLivedIn"": [
                        ""Dubai"",
                        ""AnnArbor"",
                        ""Ithaca"",
                        ""Houston"",
                        ""Seattle""],
                    ""previousJobs"":[
                        {
                            ""id"":""1"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Company"",
                            ""location"":""Houston"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander"",
                                ""Aristotle"",
                                ""Gates"",
                                ""Jobbs""]
                        },
                        {
                            ""id"":""2"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Other Company"",
                            ""location"":""Ann Arbor"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""3"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Yet Another Company"",
                            ""location"":""Ithaca"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander"",
                                ""Aristotle""]
                        },
                        {
                            ""id"":""4"",
                            ""title"":""Software Engineer"",
                            ""employer"":""One Last Company"",
                            ""location"":""Seattle"",
                            ""managers"":[
                                ""Gates"",
                                ""Bartholomew""]
                        }]
                  }");
        }

        private static JObject GetResultForNestedObjectUsingUpdateMergeOption()
        {
            return JObject.Parse(
                @"{
                    ""id"":""123"",
                    ""firstName"":""Abinav"",
                    ""lastName"":""Ramesh"",
                    ""dateOfBirth"":""23 - July - 1988"",
                    ""employer"":""Some Company"",
                    ""previousEmployer"":""Some Other Company"",
                    ""citiesLivedIn"": [
                        ""Dubai"",
                        ""AnnArbor"",
                        ""Ithaca"",
                        ""Houston"",
                        ""Seattle""],
                    ""previousJobs"":[
                        {
                            ""id"":""1"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Company"",
                            ""location"":""Houston"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""2"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Other Company"",
                            ""location"":""Ann Arbor"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""3"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Yet Another Company"",
                            ""location"":""Ithaca"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander"",
                                ""Aristotle""]
                        },
                        {
                            ""id"":""4"",
                            ""title"":""Software Engineer 2"",
                            ""employer"":""One Last Company Again"",
                            ""location"":""Houston"",
                            ""managers"":[
                                ""Gates"",
                                ""Bartholomew"",
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        }]
                  }");
        }

        private static JObject GetResultForNestedObjectUsingReplaceMergeOption()
        {
            return JObject.Parse(
                @"{
                    ""id"":""123"",
                    ""firstName"":""Abinav"",
                    ""lastName"":""Ramesh"",
                    ""dateOfBirth"":""23 - July - 1988"",
                    ""employer"":""Some Company"",
                    ""previousEmployer"":""Some Other Company"",
                    ""citiesLivedIn"": [
                        ""Dubai"",
                        ""AnnArbor"",
                        ""Ithaca"",
                        ""Houston"",
                        ""Seattle""],
                    ""previousJobs"":[
                        {
                            ""id"":""1"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Company"",
                            ""location"":""Houston"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""2"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Other Company"",
                            ""location"":""Ann Arbor"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""3"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Yet Another Company"",
                            ""location"":""Ithaca"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander"",
                                ""Aristotle""]
                        },
                        {
                            ""title"":""Software Engineer 2"",
                            ""employer"":""One Last Company Again"",
                            ""location"":""Houston"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        }]
                  }");
        }

        private static JObject GetResultForSuccessfulAdditionOfObjectToAnArrayOfObjectsUsingArrayUpdateMergeOption()
        {
            return JObject.Parse(
                @"{
                    ""id"":""123"",
                    ""firstName"":""Abinav"",
                    ""lastName"":""Ramesh"",
                    ""dateOfBirth"":""23 - July - 1988"",
                    ""employer"":""Some Company"",
                    ""previousEmployer"":""Some Other Company"",
                    ""citiesLivedIn"": [
                        ""Dubai"",
                        ""AnnArbor"",
                        ""Ithaca"",
                        ""Houston"",
                        ""Seattle""],
                    ""previousJobs"":[
                        {
                            ""id"":""1"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Company"",
                            ""location"":""Houston"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""2"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Some Other Company"",
                            ""location"":""Ann Arbor"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander""]
                        },
                        {
                            ""id"":""3"",
                            ""title"":""Software Engineer"",
                            ""employer"":""Yet Another Company"",
                            ""location"":""Ithaca"",
                            ""managers"":[
                                ""Aristotle"",
                                ""Bartholomew"",
                                ""Columbus"",
                                ""Alexander"",
                                ""Aristotle""]
                        },
                        {
                            ""id"":""4"",
                            ""title"":""Software Engineer"",
                            ""employer"":""One Last Company"",
                            ""location"":""Seattle"",
                            ""managers"":[
                                ""Gates"",
                                ""Bartholomew""]
                        },
                        {
                            ""id"":""5"",
                            ""title"":""Software Engineer 2"",
                            ""employer"":""Still Another Company"",
                            ""location"":""Helsinki"",
                            ""managers"":[
                                ""Kobe"",
                                ""Shaq"",
                                ""Kareem"",
                                ""Magic""]
                        }]
                  }");
        }

        private static PartialUpdateMergeOptions CreatePartialUpdateOptionsForRootLevelPartialUpdate()
        {
            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.UNION;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.UPDATE;            

            return partialUpdateMergeOptions;
        }

        private static PartialUpdateMergeOptions CreatePartialUpdateOptionsForRootLevelArrayUpdate()
        {
            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.CONCAT;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.UPDATE;
            partialUpdateMergeOptions.objectFilteringPropertyName = "id";
            partialUpdateMergeOptions.objectFilteringPropertyValue = "1";

            return partialUpdateMergeOptions;
        }

        private static PartialUpdateMergeOptions CreatePartialUpdateOptionsForNestedObjectUpdate()
        {
            PartialUpdateMergeOptions partialUpdateMergeOptions = new PartialUpdateMergeOptions();
            partialUpdateMergeOptions.ArrayMergeOptions = ArrayMergeOptions.CONCAT;
            partialUpdateMergeOptions.NullValueMergeOptions = NullValueMergeOptions.IGNORE;
            partialUpdateMergeOptions.ObjectMergeOptions = ObjectMergeOptions.REPLACE;
            partialUpdateMergeOptions.objectFilteringPropertyName = "id";
            partialUpdateMergeOptions.objectFilteringPropertyValue = "1";

            return partialUpdateMergeOptions;
        }

        public static async Task<DocumentCollection> RecreateCollectionAsync(
            DocumentClient client,
            string databaseName,
            string collectionName,
            bool isPartitionedCollection,
            string partitionKeyDefinition = null)
        {
            if (GetDatabaseIfExists(client, databaseName) == null)
            {
                await client.CreateDatabaseAsync(
                    new Database { Id = databaseName });
            }

            DocumentCollection collection = null;

            try
            {
                collection = await client.ReadDocumentCollectionAsync(
                        UriFactory.CreateDocumentCollectionUri(
                            databaseName,
                            collectionName))
                    .ConfigureAwait(false);
            }
            catch (DocumentClientException)
            {
            }

            if (collection != null)
            {
                await client.DeleteDocumentCollectionAsync(
                    UriFactory.CreateDocumentCollectionUri(
                        databaseName,
                        collectionName)).ConfigureAwait(false);
            }

            DocumentCollection myCollection = new DocumentCollection() { Id = collectionName };
            string partitionKey = ConfigurationManager.AppSettings["PartitionKeyName"];

            if (!string.IsNullOrWhiteSpace(partitionKey) && isPartitionedCollection)
            {
                myCollection.PartitionKey.Paths.Add(partitionKey);
            }

            myCollection.DefaultTimeToLive = -1;

            return await client.CreateDocumentCollectionAsync(
                UriFactory.CreateDatabaseUri(databaseName),
                myCollection,
                new RequestOptions { OfferThroughput = int.Parse(ConfigurationManager.AppSettings["OfferThroughput"]) });
        }

        public static Database GetDatabaseIfExists(DocumentClient client, string databaseName)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }
    }
}
