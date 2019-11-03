using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using EricBach.CQRS.Aggregate;
using EricBach.CQRS.EventRepository.Snapshots;
using EricBach.CQRS.Events;
using Newtonsoft.Json;
using Logger = EricBach.LambdaLogger.Logger;

namespace EricBach.CQRS.EventRepository.EventStore
{
    public class DynamoDbEventStore : IEventStore
    {
        private readonly AmazonDynamoDBClient _dynamoDbClient;
        private readonly string _tableName;
        private readonly List<Snapshot> _snapshots;

        public DynamoDbEventStore(string tableName)
        {
            _dynamoDbClient = new AmazonDynamoDBClient(RegionEndpoint.USWest2);

            _tableName = tableName;

            _snapshots = new List<Snapshot>();
        }

        public async Task<IEnumerable<Event>> GetEventsAsync(Guid aggregateId)
        {
            var response = await GetByIdAsync(aggregateId);

            return GetEventsFromDynamoDbResponse(response.Items);
        }

        public async Task<bool> VerifyAggregateExistsAsync(Guid aggregateId)
        {
            var response = await GetByIdAsync(aggregateId);

            return response.Items.Count > 0;
        }

        private async Task<QueryResponse> GetByIdAsync(Guid id)
        {
            Logger.Log($"Scanning DynamoDB for aggregate: {id}");

            // Read events from DynamoDB
            var request = new QueryRequest
            {
                TableName = _tableName,
                KeyConditionExpression = "Id = :v_Id and Version > :v_Version",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":v_Id", new AttributeValue { S = $"\"{id.ToString()}\"" }},
                    {":v_Version", new AttributeValue { S = "0" }}
                }
            };

            var response = await _dynamoDbClient.QueryAsync(request);

            Logger.Log($"Found items: {response.Items.Count}");

            return response;
        }

        public async Task SaveAsync(AggregateRoot aggregate)
        {
            Logger.Log($"Preparing to save aggregate {aggregate.Id} to DynamoDB");

            var uncommittedChanges = aggregate.GetUncommittedChanges();
            var version = aggregate.Version;

            foreach (var @event in uncommittedChanges)
            {
                @event.Version = ++version;
                @event.Timestamp = DateTime.UtcNow;

                // Write event to DynamoDB
                try
                {
                    var eventTable = Table.LoadTable(_dynamoDbClient, _tableName);

                    var record = new Document();
                    var prop = @event.GetType().GetMembers().Where(m => m.MemberType == MemberTypes.Property).ToList();
                    foreach (var p in prop)
                    {
                        var name = p.Name;
                        // There should only be 4 fields in an Event - Id, Version, EventName, and Event
                        if (p.Name != nameof(Event.Id) && p.Name != nameof(Event.Version) && p.Name != nameof(Event.EventName))
                        {
                            name = nameof(Event);
                            record[name] = JsonConvert.SerializeObject(@event);
                        }
                        else
                        {
                            record[name] = JsonConvert.SerializeObject(@event.GetType().GetProperty(name).GetValue(@event, null));
                        }
                    }

                    Logger.Log($"Saving aggregate to DynamoDB");
                    await eventTable.PutItemAsync(record);
                    Logger.Log($"Successfully saved aggregate to DynamoDB");
                }
                catch (Exception ex)
                {
                    // TODO Surface exception up
                    Logger.Log($"ERROR: {ex.Message}");
                    throw new Exception(ex.Message);
                }

                Logger.Log($"Checking if snapshots should be saved: {version > 2 && version % 3 == 0}");

                // Save a snapshot every 3 events
                if (version > 2 && version % 3 == 0)
                {
                    var originator = (ISnapshot)aggregate;

                    var snapshot = originator.GetSnapshot();
                    snapshot.Timestamp = DateTime.UtcNow;
                    
                    await SaveSnapshotAsync(snapshot);
                }
            }
        }

        public async Task<IEnumerable<Event>> GetAllEventsAsync()
        {
            var scanRequest = new ScanRequest
            {
                TableName = _tableName
            };

            var response = await _dynamoDbClient.ScanAsync(scanRequest);

            return GetEventsFromDynamoDbResponse(response.Items);
        }

        public async Task DeleteAllAsync()
        {
            // Delete the table
            await DeleteDynamoDbTableAsync(_tableName);

            // Re-created the table
            await CreateDynamoDbTableAsync(_tableName);
        }

        // TODO Get up to most recent snapshot and any newer events only
        private IEnumerable<Event> GetEventsFromDynamoDbResponse(IEnumerable<Dictionary<string, AttributeValue>> items)
        {
            var events = new List<Event>();

            Logger.Log($"Getting events from {items.Count()} items");

            foreach (var item in items)
            {
                var eventType = item.FirstOrDefault(i => i.Key == nameof(Event.EventName)).Value.S.Trim('"');
                Logger.Log($"Item EventType: {eventType}");
                var type = AppDomain.CurrentDomain.GetAssemblies().SelectMany(x => x.GetTypes()).First(x => x.Name == eventType);
                Logger.Log($"Item Type: {type}");

                if (type == typeof(Snapshot)) continue;

                var @event = JsonConvert.DeserializeObject(item.FirstOrDefault(i => i.Key == nameof(Event)).Value.S, type);
                Logger.Log($"Item Event: {@event}");

                events.Add((Event)@event);
            }

            return events;
        }

        private async Task DeleteDynamoDbTableAsync(string tableName)
        {
            var response = new DeleteTableResponse();

            try
            {
                var deleteResponse = _dynamoDbClient.DeleteTableAsync(tableName);
                response = await deleteResponse;
            }
            catch (Exception ex)
            {
                Logger.Log($"ERROR: Failed to delete the table, because:\n {ex.Message}");
            }

            await WaitTillTableDeletedAsync(tableName, response);
        }

        private async Task WaitTillTableDeletedAsync(string tableName, DeleteTableResponse response)
        {
            var tableDescription = response.TableDescription;

            string status = tableDescription.TableStatus;

            // Wait until table is created. Call DescribeTable
            try
            {
                while (status == "DELETING")
                {
                    System.Threading.Thread.Sleep(5000);

                    var res = await _dynamoDbClient.DescribeTableAsync(new DescribeTableRequest
                    {
                        TableName = tableName
                    });

                    status = res.Table.TableStatus;
                }
            }
            catch (ResourceNotFoundException)
            {
                // Table deleted
            }
        }

        private async Task CreateDynamoDbTableAsync(string tableName)
        {
            var response = new CreateTableResponse();

            try
            {
                var request = new CreateTableRequest
                {
                    TableName = tableName,
                    AttributeDefinitions = new List<AttributeDefinition>
                {
                    new AttributeDefinition
                    {
                        AttributeName = "Id",
                        AttributeType = "S"
                    },
                    new AttributeDefinition
                    {
                        AttributeName = "Version",
                        AttributeType = "S"
                    }
                },
                    KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement { AttributeName = "Id", KeyType = KeyType.HASH },
                    new KeySchemaElement { AttributeName = "Version", KeyType = KeyType.RANGE }
                },
                    ProvisionedThroughput = new ProvisionedThroughput
                    {
                        ReadCapacityUnits = 5,
                        WriteCapacityUnits = 5
                    },
                    StreamSpecification = new StreamSpecification
                    {
                        StreamViewType = StreamViewType.NEW_IMAGE,
                        StreamEnabled = true
                    }
                };

                var createTableResponse = _dynamoDbClient.CreateTableAsync(request);
                response = await createTableResponse;
            }
            catch (Exception ex)
            {
                Logger.Log($"ERROR: Failed to create the new table, because: {ex.Message}");
            }

            await WaitTillTableCreatedAsync(tableName, response);
        }

        private async Task WaitTillTableCreatedAsync(string tableName, CreateTableResponse response)
        {
            var tableDescription = response.TableDescription;

            string status = tableDescription.TableStatus;

            // Wait until table is created
            while (status != "ACTIVE")
            {
                System.Threading.Thread.Sleep(5000);

                try
                {
                    var res = await _dynamoDbClient.DescribeTableAsync(new DescribeTableRequest
                    {
                        TableName = tableName
                    });

                    status = res.Table.TableStatus;
                }
                catch (ResourceNotFoundException)
                {
                    // Try-catch to handle potential eventual-consistency issue
                }
            }
        }

        public T GetSnapshot<T>(Guid aggregateId) where T : Snapshot
        {
            var snapshot = _snapshots.Where(m => m.Id == aggregateId).Select(m => m).LastOrDefault();

            return (T)snapshot;
        }

        public async Task SaveSnapshotAsync(Snapshot snapshot)
        {
            try
            {
                var eventTable = Table.LoadTable(_dynamoDbClient, _tableName);

                var record = new Document
                {
                    ["Id"] = JsonConvert.SerializeObject(snapshot.Id),
                    ["Version"] = "Snapshot-" + (snapshot.Version + 1),
                    ["Event"] = JsonConvert.SerializeObject(snapshot),
                    ["EventName"] = "Snapshot"
                };

                Logger.Log("Saving snapshot to DynamoDB");
                await eventTable.PutItemAsync(record);
                Logger.Log("Successfully saved snapshot to DynamoDB");
            }
            catch (Exception ex)
            {
                // TODO Surface exception up
                Logger.Log($"ERROR: {ex.Message}");
                throw new Exception(ex.Message);
            }
        }
    }
}
