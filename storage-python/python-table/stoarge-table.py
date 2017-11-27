from azure.storage.table import TableService, Entity
from azure.storage.table import TableBatch

# create table first
tablename = 'tasktable'
table_service = TableService(account_name = "[account key here]", account_key = "[account key here]")
table_service.create_table(tablename)

# insert entity
def insertEntity():   
    deleteTable()
    task = Entity()
    task.PartitionKey = 'tasksSeattle'
    task.RowKey = '002'
    task.description = 'Wash the car'
    task.priority = 100
    table_service.insert_entity(tablename, task)

# update entity
def updateEntity():
    task = {'PartitionKey': 'tasksSeattle', 'RowKey': '001', 'description' : 'Take out the garbage', 'priority' : 250}
    table_service.update_entity(tablename, task)

# insert or replace entity
def insertOrReplaceEntity():
    # Replace the entity created earlier
    task = {'PartitionKey': 'tasksSeattle', 'RowKey': '001', 'description' : 'Take out the garbage again', 'priority' : 250}
    table_service.insert_or_replace_entity(tablename, task)

    # Insert a new entity
    task = {'PartitionKey': 'tasksSeattle', 'RowKey': '003', 'description' : 'Buy detergent', 'priority' : 300}
    table_service.insert_or_replace_entity(tablename, task)

# modify entity
def modifyEntity():  
    batch = TableBatch()
    task004 = {'PartitionKey': 'tasksSeattle', 'RowKey': '004', 'description' : 'Go grocery shopping', 'priority' : 400}
    task005 = {'PartitionKey': 'tasksSeattle', 'RowKey': '005', 'description' : 'Clean the bathroom', 'priority' : 100}
    batch.insert_entity(task004)
    batch.insert_entity(task005)
    table_service.commit_batch(tablename, batch)

# quert entity
def queryEntity():
    task = table_service.get_entity(tablename, 'tasksSeattle', '001')
    print(task.description)
    print(task.priority)

# query collection entities
def queryCollectionEntity():
    tasks = table_service.query_entities(tablename, filter="PartitionKey eq 'tasksSeattle'")
    for task in tasks:
        print(task.description)
        print(task.priority)

# query part entities
def queryPartEntity():
    tasks = table_service.query_entities(tablename, filter="PartitionKey eq 'tasksSeattle'", select='description')
    for task in tasks:
        print(task.description)

# delete entity
def deleteEntity():
    table_service.delete_entity(tablename, 'tasksSeattle', '001')

# delete table
def deleteTable():
    table_service.delete_table(tablename)


# run the sample
if __name__ == "__main__":
    print ('This is main of module "storage-table.py"')        
    insertEntity()
    modifyEntity()
    queryEntity()
