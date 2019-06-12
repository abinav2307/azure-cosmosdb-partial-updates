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

            // Assert the value of updated field
            Assert.AreEqual(updatedDocument.Resource.GetPropertyValue<string>("employer"), "Some Other Company");
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

            // Assert the value of the fields to be updated, prior to the update
            Assert.AreEqual(originalDocumentPriorToPartialUpdate.Resource.GetPropertyValue<string>("employer"), "Some Company");
            Assert.AreEqual(originalDocumentPriorToPartialUpdate.Resource.GetPropertyValue<string>("previousEmployer"), "Some Other Company");

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

            // Assert the update value of 'employer'
            Assert.AreEqual(updatedDocument.Resource.GetPropertyValue<string>("employer"), "Some Other Company");

            // Assert the update value of 'previousEmployer'
            Assert.AreEqual(updatedDocument.Resource.GetPropertyValue<string>("previousEmployer"), "Some Company");
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

            JArray previousJobs = originalDocumentPriorToPartialUpdate.Resource.GetPropertyValue<JArray>("previousJobs");
            foreach (JObject eachObject in previousJobs)
            {
                if(((string)eachObject["id"]).Equals("1"))
                {
                    JArray arrayOfManagers = (JArray)eachObject["managers"];
                    Assert.AreEqual(arrayOfManagers.Count, 4);
                }
            }

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

            List<string> updatedArrayOfManagers = new List<string>(){ "Aristotle", "Bartholomew", "Columbus", "Alexander", "Gates", "Jobbs" };
            JArray previousJobsUpdated = updatedDocument.Resource.GetPropertyValue<JArray>("previousJobs");
            foreach (JObject eachObject in previousJobsUpdated)
            {
                if (((string)eachObject["id"]).Equals("1"))
                {
                    JArray arrayOfManagers = (JArray)eachObject["managers"];
                    Assert.AreEqual(arrayOfManagers.Count, 6);

                    foreach (JToken eachElementInArrayOfManagers in arrayOfManagers.Children())
                    {
                        var match = updatedArrayOfManagers.First(stringToCheck => stringToCheck.Contains(eachElementInArrayOfManagers.ToObject<string>()));
                        Assert.IsNotNull(match);
                    }
                }
            }            
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

            JArray previousJobs = originalDocumentPriorToPartialUpdate.Resource.GetPropertyValue<JArray>("previousJobs");
            foreach (JObject eachObject in previousJobs)
            {
                if (((string)eachObject["id"]).Equals("1"))
                {
                    JArray arrayOfManagers = (JArray)eachObject["managers"];
                    Assert.AreEqual(arrayOfManagers.Count, 4);
                }
            }

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

            List<string> updatedArrayOfManagers = new List<string>() { "Aristotle", "Bartholomew", "Columbus", "Alexander", "Gates", "Jobbs" };
            JArray previousJobsUpdated = updatedDocument.Resource.GetPropertyValue<JArray>("previousJobs");
            foreach (JObject eachObject in previousJobsUpdated)
            {
                if (((string)eachObject["id"]).Equals("1"))
                {
                    JArray arrayOfManagers = (JArray)eachObject["managers"];
                    Assert.AreEqual(arrayOfManagers.Count, 7);

                    foreach (JToken eachElementInArrayOfManagers in arrayOfManagers.Children())
                    {
                        var match = updatedArrayOfManagers.First(stringToCheck => stringToCheck.Contains(eachElementInArrayOfManagers.ToObject<string>()));
                        Assert.IsNotNull(match);
                    }
                }
            }
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

            JArray previousJobs = originalDocumentPriorToPartialUpdate.Resource.GetPropertyValue<JArray>("previousJobs");
            foreach (JObject eachObject in previousJobs)
            {
                if (((string)eachObject["id"]).Equals("1"))
                {
                    JArray arrayOfManagers = (JArray)eachObject["managers"];
                    Assert.AreEqual(arrayOfManagers.Count, 4);
                }
            }

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

            List<string> updatedArrayOfManagers = new List<string>() { "Aristotle", "Gates", "Jobbs" };
            JArray previousJobsUpdated = updatedDocument.Resource.GetPropertyValue<JArray>("previousJobs");
            foreach (JObject eachObject in previousJobsUpdated)
            {
                if (((string)eachObject["id"]).Equals("1"))
                {
                    JArray arrayOfManagers = (JArray)eachObject["managers"];
                    Assert.AreEqual(arrayOfManagers.Count, 3);

                    foreach (JToken eachElementInArrayOfManagers in arrayOfManagers.Children())
                    {
                        var match = updatedArrayOfManagers.First(stringToCheck => stringToCheck.Contains(eachElementInArrayOfManagers.ToObject<string>()));
                        Assert.IsNotNull(match);
                    }
                }
            }
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

            JArray previousJobs = originalDocumentPriorToPartialUpdate.Resource.GetPropertyValue<JArray>("previousJobs");
            foreach (JObject eachObject in previousJobs)
            {
                if (((string)eachObject["id"]).Equals("1"))
                {
                    JArray arrayOfManagers = (JArray)eachObject["managers"];
                    Assert.AreEqual(arrayOfManagers.Count, 4);
                }
            }

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

            List<string> updatedArrayOfManagers = new List<string>() { "Aristotle", "Bartholomew", "Columbus", "Alexander", "Gates", "Jobbs" };
            JArray previousJobsUpdated = updatedDocument.Resource.GetPropertyValue<JArray>("previousJobs");
            foreach (JObject eachObject in previousJobsUpdated)
            {
                if (((string)eachObject["id"]).Equals("1"))
                {
                    JArray arrayOfManagers = (JArray)eachObject["managers"];
                    Assert.AreEqual(arrayOfManagers.Count, 7);

                    foreach (JToken eachElementInArrayOfManagers in arrayOfManagers.Children())
                    {
                        var match = updatedArrayOfManagers.First(stringToCheck => stringToCheck.Contains(eachElementInArrayOfManagers.ToObject<string>()));
                        Assert.IsNotNull(match);
                    }
                }
            }
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

            JArray previousJobsUpdated = updatedDocument.Resource.GetPropertyValue<JArray>("previousJobs");
            foreach (JObject eachObject in previousJobsUpdated)
            {
                // Since the object was replaced, id should have been removed
                // since id was not present in the payload of the partial update
                if (eachObject["id"] == null)
                {
                    Assert.AreEqual((string)eachObject["title"], "Software Engineer 2");
                    Assert.AreEqual((string)eachObject["employer"], "One Last Company Again");
                    Assert.AreEqual((string)eachObject["location"], "Houston");

                    List<string> updatedArrayOfManagers = new List<string>() { "Aristotle", "Bartholomew", "Columbus", "Alexander"};
                    JArray arrayOfManagers = (JArray)eachObject["managers"];
                    foreach (JToken eachElementInArrayOfManagers in arrayOfManagers.Children())
                    {
                        var match = updatedArrayOfManagers.First(stringToCheck => stringToCheck.Contains(eachElementInArrayOfManagers.ToObject<string>()));
                        Assert.IsNotNull(match);
                    }
                }
            }
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

            JArray previousJobsUpdated = updatedDocument.Resource.GetPropertyValue<JArray>("previousJobs");
            foreach (JObject eachObject in previousJobsUpdated)
            {
                // Since the object was update, id should NOT have been removed
                // even though id was not present in the payload of the partial update
                if (((string)eachObject["id"]).Equals("4"))
                {
                    Assert.AreEqual((string)eachObject["title"], "Software Engineer 2");
                    Assert.AreEqual((string)eachObject["employer"], "One Last Company Again");
                    Assert.AreEqual((string)eachObject["location"], "Houston");

                    List<string> updatedArrayOfManagers = new List<string>() { "Gates", "Aristotle", "Bartholomew", "Columbus", "Alexander" };
                    JArray arrayOfManagers = (JArray)eachObject["managers"];
                    foreach (JToken eachElementInArrayOfManagers in arrayOfManagers.Children())
                    {
                        var match = updatedArrayOfManagers.First(stringToCheck => stringToCheck.Contains(eachElementInArrayOfManagers.ToObject<string>()));
                        Assert.IsNotNull(match);
                    }
                }
            }
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

        private static JObject GetSampleDocument()
        {
            //return JObject.Parse("{\"id\":\"123\",\"firstName\":\"Abinav\",\"lastName\":\"Ramesh\",\"dateOfBirth\":\"23 - July - 1988\",\"employer\":\"AlaskaAirlines\",\"previousEmployer\":\"PROS,inc ? \",\"citiesLivedIn\":[\"Dubai\",\"AnnArbor\",\"Ithaca\",\"Houston\",\"Seattle\"],\"previousJobs\":[{\"id\":\"1\",\"bookingClasses\":[{\"id\":\"123456789\",\"totalSeatsAvailable\":75,\"totalBookings\":30,\"grouBookings\":12,\"compartmentCodes\":[{\"id\":\"1234567890\",\"name\":\"Z\",\"class\":\"Business\"}]}]},{\"id\":\"2\",\"title\":\"SoftwareEngineer1\",\"employer\":\"PROS,INC\",\"location\":\"Houston\",\"managers\":[\"Aristotle\",\"Bartholomew\",\"Columbus\",\"Alexander\"]},{\"id\":\"3\",\"title\":\"SoftwareEngineer1\",\"employer\":\"MicrosoftCorporation\",\"location\":\"Seattle\",\"managers\":[\"Aristotle\",\"Bartholomew\",\"Columbus\",\"Alexander\",\"Aristotle\"]},{\"id\":\"4\",\"title\":\"SoftwareEngineer2\",\"employer\":\"MicrosoftCorporation\",\"location\":\"Seattle\",\"managers\":[\"Gates\",\"Bartholomey\"],\"previousEmployer\":\"PROS\"}]}");
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

        private static JObject GetSamplePartialUpdateDocumentWithArray()
        {
            return JObject.Parse("{\"managers\": [\"Aristotle\",\"Gates\",\"Jobbs\"]}");
        }

        private static JObject GetSamplePartialUpdateDocumentWithNestedObject()
        {
            return JObject.Parse("{\"title\":\"Software Engineer 1\",\"employer\":\"PROS,INC\",\"location\":\"Houston\",\"managers\":[\"Aristotle\",\"Bartholomew\",\"Columbus\",\"Alexander\"]}");
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
