package com.storage.table.wing;

import java.io.*;
import java.util.Date;
import java.util.HashMap;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.table.*;
import com.microsoft.azure.storage.table.TableQuery.*;

public class storageTable {
	
	// Define the connection-string with your values.
		public static final String storageConnectionString =
		    "DefaultEndpointsProtocol=http;" +
		    "AccountName=your_storage_account;" +
		    "AccountKey=your_storage_account_key";
	
	
	// Retrieve storage account from connection-string.
	//	public static final String storageConnectionString = RoleEnvironment.getConfigurationSettings().get("StorageConnectionString");
	//	
	
	public static void main(String[] args) {
				
		// create table
		createTable();
		
		// list table
		listTable();
		
		// add entity
		addEntity();
		
		// add entities by batching
		addBatchEntities();
		
		// query all entities
		queryAllEntities();
		
		// query part entity
		queryPartEntity();
		
		// query single entity
		querySingleEntity();
		
		// update entity
		updateEntity();	
		
		
		// others

	}
	
	private static void createTable()
	{
		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Create the table if it doesn't exist.
		    String tableName = "people";
		    CloudTable cloudTable = tableClient.getTableReference(tableName);
		    cloudTable.createIfNotExists();
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
	}

	
	private static void listTable()
	{
		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Loop through the collection of table names.
		    for (String table : tableClient.listTables())
		    {
		        // Output each table name.
		        System.out.println(table);
		    }
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
	}
	
	private static void addEntity()
	{
		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Create a cloud table object for the table.
		    CloudTable cloudTable = tableClient.getTableReference("people");

		    // Create a new customer entity.
		    CustomerEntity customer1 = new CustomerEntity("Harp", "Walter");
		    customer1.setEmail("Walter@contoso.com");
		    customer1.setPhoneNumber("425-555-0101");

		    // Create an operation to add the new customer to the people table.
		    TableOperation insertCustomer1 = TableOperation.insertOrReplace(customer1);

		    // Submit the operation to the table service.
		    cloudTable.execute(insertCustomer1);
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
	
	}
	
	private static void addBatchEntities()
	{
		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Define a batch operation.
		    TableBatchOperation batchOperation = new TableBatchOperation();

		    // Create a cloud table object for the table.
		    CloudTable cloudTable = tableClient.getTableReference("people");

		    // Create a customer entity to add to the table.
		    CustomerEntity customer = new CustomerEntity("Smith", "Jeff");
		    customer.setEmail("Jeff@contoso.com");
		    customer.setPhoneNumber("425-555-0104");
		    batchOperation.insertOrReplace(customer);

		    // Create another customer entity to add to the table.
		    CustomerEntity customer2 = new CustomerEntity("Smith", "Ben");
		    customer2.setEmail("Ben@contoso.com");
		    customer2.setPhoneNumber("425-555-0102");
		    batchOperation.insertOrReplace(customer2);

		    // Create a third customer entity to add to the table.
		    CustomerEntity customer3 = new CustomerEntity("Smith", "Denise");
		    customer3.setEmail("Denise@contoso.com");
		    customer3.setPhoneNumber("425-555-0103");
		    batchOperation.insertOrReplace(customer3);

		    // Execute the batch of operations on the "people" table.
		    cloudTable.execute(batchOperation);
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
	}
	
	private static void queryAllEntities()
	{
		try
		{
		    // Define constants for filters.
		    final String PARTITION_KEY = "PartitionKey";
		    final String ROW_KEY = "RowKey";
		    final String TIMESTAMP = "Timestamp";

		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Create a cloud table object for the table.
		    CloudTable cloudTable = tableClient.getTableReference("people");

		    // Create a filter condition where the partition key is "Smith".
		    String partitionFilter = TableQuery.generateFilterCondition(
		        PARTITION_KEY,
		        QueryComparisons.EQUAL,
		        "Smith");

		    // Specify a partition query, using "Smith" as the partition key filter.
		    TableQuery<CustomerEntity> partitionQuery =
		        TableQuery.from(CustomerEntity.class)
		        .where(partitionFilter);

		    // Loop through the results, displaying information about the entity.
		    for (CustomerEntity entity : cloudTable.execute(partitionQuery)) {
		        System.out.println(entity.getPartitionKey() +
		            " " + entity.getRowKey() +
		            "\t" + entity.getEmail() +
		            "\t" + entity.getPhoneNumber());
		    }
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
	}
	
	private static void queryPartEntity()
	{
		try
		{
		    // Define constants for filters.
		    final String PARTITION_KEY = "PartitionKey";
		    final String ROW_KEY = "RowKey";
		    final String TIMESTAMP = "Timestamp";

		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Create a cloud table object for the table.
		    CloudTable cloudTable = tableClient.getTableReference("people");

		    // Create a filter condition where the partition key is "Smith".
		    String partitionFilter = TableQuery.generateFilterCondition(
		        PARTITION_KEY,
		        QueryComparisons.EQUAL,
		        "Smith");

		    // Create a filter condition where the row key is less than the letter "E".
		    String rowFilter = TableQuery.generateFilterCondition(
		        ROW_KEY,
		        QueryComparisons.LESS_THAN,
		        "E");

		    // Combine the two conditions into a filter expression.
		    String combinedFilter = TableQuery.combineFilters(partitionFilter,
		        Operators.AND, rowFilter);

		    // Specify a range query, using "Smith" as the partition key,
		    // with the row key being up to the letter "E".
		    TableQuery<CustomerEntity> rangeQuery =
		        TableQuery.from(CustomerEntity.class)
		        .where(combinedFilter);

		    // Loop through the results, displaying information about the entity
		    for (CustomerEntity entity : cloudTable.execute(rangeQuery)) {
		        System.out.println(entity.getPartitionKey() +
		            " " + entity.getRowKey() +
		            "\t" + entity.getEmail() +
		            "\t" + entity.getPhoneNumber());
		    }
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
	}
	
	private static void querySingleEntity()
	{
		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Create a cloud table object for the table.
		    CloudTable cloudTable = tableClient.getTableReference("people");

		    // Retrieve the entity with partition key of "Smith" and row key of "Jeff"
		    TableOperation retrieveSmithJeff =
		        TableOperation.retrieve("Smith", "Jeff", CustomerEntity.class);

		    // Submit the operation to the table service and get the specific entity.
		    CustomerEntity specificEntity =
		        cloudTable.execute(retrieveSmithJeff).getResultAsType();

		    // Output the entity.
		    if (specificEntity != null)
		    {
		        System.out.println(specificEntity.getPartitionKey() +
		            " " + specificEntity.getRowKey() +
		            "\t" + specificEntity.getEmail() +
		            "\t" + specificEntity.getPhoneNumber());
		    }
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
	}
	
	private static void updateEntity()
	{
		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Create a cloud table object for the table.
		    CloudTable cloudTable = tableClient.getTableReference("people");

		    // Retrieve the entity with partition key of "Smith" and row key of "Jeff".
		    TableOperation retrieveSmithJeff =
		        TableOperation.retrieve("Smith", "Jeff", CustomerEntity.class);

		    // Submit the operation to the table service and get the specific entity.
		    CustomerEntity specificEntity =
		        cloudTable.execute(retrieveSmithJeff).getResultAsType();

		    // Specify a new phone number.
		    specificEntity.setPhoneNumber("425-555-0105");

		    // Create an operation to replace the entity.
		    TableOperation replaceEntity = TableOperation.replace(specificEntity);

		    // Submit the operation to the table service.
		    cloudTable.execute(replaceEntity);
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
	}
	
	private static void querySubEntity()
	{
		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Create a cloud table object for the table.
		    CloudTable cloudTable = tableClient.getTableReference("people");

		    // Define a projection query that retrieves only the Email property
		    TableQuery<CustomerEntity> projectionQuery =
		        TableQuery.from(CustomerEntity.class)
		        .select(new String[] {"Email"});

		    // Define a Entity resolver to project the entity to the Email value.
		    EntityResolver<String> emailResolver = new EntityResolver<String>() {
		        @Override
		        public String resolve(String PartitionKey, String RowKey, Date timeStamp, HashMap<String, EntityProperty> properties, String etag) {
		            return properties.get("Email").getValueAsString();
		        }
		    };

		    // Loop through the results, displaying the Email values.
		    for (String projectedString :
		        cloudTable.execute(projectionQuery, emailResolver)) {
		            System.out.println(projectedString);
		    }
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
	}
	
	private static void insertOrreplaceEntity()
	{
		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Create a cloud table object for the table.
		    CloudTable cloudTable = tableClient.getTableReference("people");

		    // Create a new customer entity.
		    CustomerEntity customer5 = new CustomerEntity("Harp", "Walter");
		    customer5.setEmail("Walter@contoso.com");
		    customer5.setPhoneNumber("425-555-0106");

		    // Create an operation to add the new customer to the people table.
		    TableOperation insertCustomer5 = TableOperation.insertOrReplace(customer5);

		    // Submit the operation to the table service.
		    cloudTable.execute(insertCustomer5);
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
	}
	
	private static void deleteEntity()
	{
		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Create a cloud table object for the table.
		    CloudTable cloudTable = tableClient.getTableReference("people");

		    // Create an operation to retrieve the entity with partition key of "Smith" and row key of "Jeff".
		    TableOperation retrieveSmithJeff = TableOperation.retrieve("Smith", "Jeff", CustomerEntity.class);

		    // Retrieve the entity with partition key of "Smith" and row key of "Jeff".
		    CustomerEntity entitySmithJeff =
		        cloudTable.execute(retrieveSmithJeff).getResultAsType();

		    // Create an operation to delete the entity.
		    TableOperation deleteSmithJeff = TableOperation.delete(entitySmithJeff);

		    // Submit the delete operation to the table service.
		    cloudTable.execute(deleteSmithJeff);
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
	}
	
	private static void deleteTable()
	{
		try
		{
		    // Retrieve storage account from connection-string.
		    CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);

		    // Create the table client.
		    CloudTableClient tableClient = storageAccount.createCloudTableClient();

		    // Delete the table and all its data if it exists.
		    CloudTable cloudTable = tableClient.getTableReference("people");
		    cloudTable.deleteIfExists();
		}
		catch (Exception e)
		{
		    // Output the stack trace.
		    e.printStackTrace();
		}
	}
}
