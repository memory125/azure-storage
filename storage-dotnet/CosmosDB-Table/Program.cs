using System;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;


namespace CosmosDB_Table
{
    class Program
    {
        static void Main(string[] args)
        {
            // Create Table and Insert Entity
            InsertBatchEntity();

            // Query All Entities
            QueryAllEntities();

            // Quert Part of Entities
            QueryPartEntity();

            // Query Single Entity
            QuerySingleEntity();

            // Replace Entity
            ReplaceEntity();

            // Insert or Replace Entity
            InsertOrReplaceEntity();

            // Query Partitial of Entities
            QueryPartitialEntity();
        }

        private static void InsertBatchEntity()
        {
            // Retrieve the storage account from the connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference("people");

            // Create the table if it doesn't exist.
            table.CreateIfNotExists();

            // Create the batch operation.
            TableBatchOperation batchOperation = new TableBatchOperation();

            // Create a customer entity and add it to the table.
            CustomerEntity customer1 = new CustomerEntity("Smith", "Jeff");
            customer1.Email = "Jeff@contoso.com";
            customer1.PhoneNumber = "425-555-0104";

            // Create another customer entity and add it to the table.
            CustomerEntity customer2 = new CustomerEntity("Smith", "Ben");
            customer2.Email = "Ben@contoso.com";
            customer2.PhoneNumber = "425-555-0102";

            // Add both customer entities to the batch insert operation.
            batchOperation.Insert(customer1);
            batchOperation.Insert(customer2);

            try
            {
                // Execute the batch operation.
                table.ExecuteBatch(batchOperation);
            }catch (Exception ex)
            {
                Console.WriteLine("Exception is " + ex.Message);
            }
        }

        private static void QueryAllEntities()
        {
            // Retrieve the storage account from the connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference("people");

            // Construct the query operation for all customer entities where PartitionKey="Smith".
            TableQuery<CustomerEntity> query = new TableQuery<CustomerEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Smith"));

            // Print the fields for each customer.
            foreach (CustomerEntity entity in table.ExecuteQuery(query))
            {
                Console.WriteLine("{0}, {1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey,
                    entity.Email, entity.PhoneNumber);
            }
        }

        private static void QueryPartEntity()
        {
            // Retrieve the storage account from the connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference("people");

            // Create the table query.
            TableQuery<CustomerEntity> rangeQuery = new TableQuery<CustomerEntity>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Smith"),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, "E")));

            // Loop through the results, displaying information about the entity.
            foreach (CustomerEntity entity in table.ExecuteQuery(rangeQuery))
            {
                Console.WriteLine("{0}, {1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey,
                    entity.Email, entity.PhoneNumber);
            }
        }

        private static void QuerySingleEntity()
        {
            // Retrieve the storage account from the connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference("people");

            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>("Smith", "Ben");

            // Execute the retrieve operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Print the phone number of the result.
            if (retrievedResult.Result != null)
            {
                Console.WriteLine(((CustomerEntity)retrievedResult.Result).PhoneNumber);
            }
            else
            {
                Console.WriteLine("The phone number could not be retrieved.");
            }
        }

        private static void ReplaceEntity()
        {
            // Retrieve the storage account from the connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference("people");

            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>("Smith", "Ben");

            // Execute the operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Assign the result to a CustomerEntity object.
            CustomerEntity updateEntity = (CustomerEntity)retrievedResult.Result;

            if (updateEntity != null)
            {
                // Change the phone number.
                updateEntity.PhoneNumber = "425-555-0105";

                // Create the Replace TableOperation.
                TableOperation updateOperation = TableOperation.Replace(updateEntity);

                // Execute the operation.
                table.Execute(updateOperation);

                Console.WriteLine("Entity updated.");
            }
            else
            {
                Console.WriteLine("Entity could not be retrieved.");
            }
        }

        private static void InsertOrReplaceEntity()
        {
            // Retrieve the storage account from the connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference("people");

            // Create a customer entity.
            CustomerEntity customer3 = new CustomerEntity("Jones", "Fred");
            customer3.Email = "Fred@contoso.com";
            customer3.PhoneNumber = "425-555-0106";

            // Create the TableOperation object that inserts the customer entity.
            TableOperation insertOperation = TableOperation.Insert(customer3);

            // Execute the operation.
            table.Execute(insertOperation);

            // Create another customer entity with the same partition key and row key.
            // We've already created a 'Fred Jones' entity and saved it to the
            // 'people' table, but here we're specifying a different value for the
            // PhoneNumber property.
            CustomerEntity customer4 = new CustomerEntity("Jones", "Fred");
            customer4.Email = "Fred@contoso.com";
            customer4.PhoneNumber = "425-555-0107";

            // Create the InsertOrReplace TableOperation.
            TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(customer4);

            // Execute the operation. Because a 'Fred Jones' entity already exists in the
            // 'people' table, its property values will be overwritten by those in this
            // CustomerEntity. If 'Fred Jones' didn't already exist, the entity would be
            // added to the table.
            table.Execute(insertOrReplaceOperation);
        }

        private static void QueryPartitialEntity()
        {
            // Retrieve the storage account from the connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable that represents the "people" table.
            CloudTable table = tableClient.GetTableReference("people");

            // Define the query, and select only the Email property.
            TableQuery<DynamicTableEntity> projectionQuery = new TableQuery<DynamicTableEntity>().Select(new string[] { "Email" });

            // Define an entity resolver to work with the entity after retrieval.
            EntityResolver<string> resolver = (pk, rk, ts, props, etag) => props.ContainsKey("Email") ? props["Email"].StringValue : null;

            foreach (string projectedEmail in table.ExecuteQuery(projectionQuery, resolver, null, null))
            {
                Console.WriteLine(projectedEmail);
            }
        }

        private static void DeleteEntity()
        {
            // Retrieve the storage account from the connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable that represents the "people" table.
            CloudTable table = tableClient.GetTableReference("people");

            // Create a retrieve operation that expects a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>("Smith", "Ben");

            // Execute the operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Assign the result to a CustomerEntity.
            CustomerEntity deleteEntity = (CustomerEntity)retrievedResult.Result;

            // Create the Delete TableOperation.
            if (deleteEntity != null)
            {
                TableOperation deleteOperation = TableOperation.Delete(deleteEntity);

                // Execute the operation.
                table.Execute(deleteOperation);

                Console.WriteLine("Entity deleted.");
            }
            else
            {
                Console.WriteLine("Could not retrieve the entity.");
            }
        }

        private static void DeleteTable()
        {
            // Retrieve the storage account from the connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable that represents the "people" table.
            CloudTable table = tableClient.GetTableReference("people");

            // Delete the table it if exists.
            table.DeleteIfExists();
        }

        private static async void QueryEntity()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
               CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable that represents the "people" table.
            CloudTable table = tableClient.GetTableReference("people");

            // Initialize a default TableQuery to retrieve all the entities in the table.
            TableQuery<CustomerEntity> tableQuery = new TableQuery<CustomerEntity>();

            // Initialize the continuation token to null to start from the beginning of the table.
            TableContinuationToken continuationToken = null;

            do
            {
                // Retrieve a segment (up to 1,000 entities).
                TableQuerySegment<CustomerEntity> tableQueryResult =
                    await table.ExecuteQuerySegmentedAsync(tableQuery, continuationToken);

                // Assign the new continuation token to tell the service where to
                // continue on the next iteration (or null if it has reached the end).
                continuationToken = tableQueryResult.ContinuationToken;

                // Print the number of rows retrieved.
                Console.WriteLine("Rows retrieved {0}", tableQueryResult.Results.Count);

                // Loop until a null continuation token is received, indicating the end of the table.
            } while (continuationToken != null);
        }
    }
}
