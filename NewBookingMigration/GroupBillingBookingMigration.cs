using System.Linq;
using Couchbase;
using Couchbase.KeyValue;
using Couchbase.Query;
using Newtonsoft.Json.Linq;

namespace NewBookingMigration;

internal class GroupBillingBookingMigration
{
    private readonly MigrationConfig _config;
    
    private static Dictionary<string, uint> _idMapping = new(); // Old GBB ID -> New Booking ID

    private const string ID_MAPPING_FILE_NAME = "gbb_to_booking_id_mapping.json";
    private const string ERROR_BOOKINGS_FILE_NAME = "error_gbb_migration_report.txt";
    private const string MAX_ID = "maxId";
    private const string TOTAL = "total";
    private const string MIGRATION_USER = "migration";

    public GroupBillingBookingMigration(MigrationConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    public async Task RunMigrationAsync()
    {
        try
        {
            Console.WriteLine("Starting Group Billing Booking migration process...");
            
            // Initialize Couchbase connection
            var cluster = await InitializeCouchbaseConnection();
            var bucket = await GetBucket(cluster);
            
            // Get current booking counter
            var currentCounter = await GetBookingCounter(cluster, bucket);
            Console.WriteLine($"📊 Current Booking counter: {currentCounter}");
            
            // Get all Group Billing Bookings
            var gbbIds = await GetAllGroupBillingBookingIds(cluster);
            Console.WriteLine($"📊 Found {gbbIds.Count} Group Billing Bookings to migrate");
            
            if (gbbIds.Count == 0)
            {
                Console.WriteLine("No Group Billing Bookings found to migrate.");
                await cluster.DisposeAsync();
                return;
            }
            
            // Process migrations in batches
            var results = await ProcessGroupBillingBookings(cluster, bucket, gbbIds, currentCounter);
            
            // Save ID mapping to file
            await SaveIdMapping();
            
            // Display final results
            Console.WriteLine($"\n✅ Migration completed!");
            Console.WriteLine($"✅ Successfully migrated: {results.successCount}");
            Console.WriteLine($"❌ Errors: {results.errorCount}");
            Console.WriteLine($"📝 ID Mapping saved to: {GetLogFilePath(ID_MAPPING_FILE_NAME)}");
            
            // Update booking counter
            await UpdateBookingCounter(cluster, bucket);
            
            // Cleanup
            await cluster.DisposeAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Migration failed: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
        }
    }

    private async Task<ICluster> InitializeCouchbaseConnection()
    {
        Console.WriteLine("Connecting to Couchbase...");
        var cluster = await Cluster.ConnectAsync(_config.ConnectionString, options =>
        {
            options.UserName = _config.Username;
            options.Password = _config.Password;
            options.EnableTls = true;
            options.EnableDnsSrvResolution = true;
            options.KvCertificateCallbackValidation = (sender, certificate, chain, sslPolicyErrors) => true;
            options.HttpCertificateCallbackValidation = (sender, certificate, chain, sslPolicyErrors) => true;
        });
        Console.WriteLine("Connected to Couchbase successfully!");
        return cluster;
    }

    private async Task<IBucket> GetBucket(ICluster cluster)
    {
        var bucket = await cluster.BucketAsync(_config.BucketName);
        await bucket.WaitUntilReadyAsync(TimeSpan.FromSeconds(10));
        return bucket;
    }

    private async Task<uint> GetBookingCounter(ICluster cluster, IBucket bucket)
    {
        try
        {
            var scopeObj = await bucket.ScopeAsync(_config.Scope);
            var collection = await scopeObj.CollectionAsync(_config.TargetCollection);
            var counterKey = "Counter";
            
            try
            {
                var counterResult = await collection.GetAsync(counterKey);
                return counterResult.ContentAs<uint>();
            }
            catch
            {
                // If counter doesn't exist, get max ID from bookings
                var maxIdQuery = $"SELECT MAX(id) as {MAX_ID} FROM `{_config.BucketName}`.`{_config.Scope}`.`{_config.TargetCollection}`";
                var maxIdResult = await cluster.QueryAsync<JObject>(maxIdQuery);
                
                uint maxId = 0;
                await foreach (var row in maxIdResult)
                {
                    if (row != null && row[MAX_ID] != null && row[MAX_ID].Type != JTokenType.Null)
                    {
                        maxId = row[MAX_ID].Value<uint>();
                    }
                }
                
                return maxId;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Failed to get booking counter: {ex.Message}");
            return 0;
        }
    }

    private async Task<List<string>> GetAllGroupBillingBookingIds(ICluster cluster)
    {
        var gbbIds = new List<string>();
        
        try
        {
            var query = $"SELECT id FROM `{_config.BucketName}`.`{_config.Scope}`.`{_config.SourceCollection}`";
            var result = await cluster.QueryAsync<JObject>(query);
            
            await foreach (var row in result)
            {
                if (row != null && row["id"] != null)
                {
                    gbbIds.Add(row["id"].ToString());
                }
            }
            
            // Sort IDs numerically if possible, otherwise alphabetically
            gbbIds.Sort((a, b) =>
            {
                // Try numeric comparison first
                if (uint.TryParse(a, out uint numA) && uint.TryParse(b, out uint numB))
                {
                    return numA.CompareTo(numB);
                }
                // Fall back to string comparison
                return string.Compare(a, b, StringComparison.Ordinal);
            });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Failed to fetch Group Billing Booking IDs: {ex.Message}");
        }
        
        return gbbIds;
    }

    private async Task<(int successCount, int errorCount)> ProcessGroupBillingBookings(
        ICluster cluster, IBucket bucket, List<string> gbbIds, uint startCounter)
    {
        int successCount = 0;
        int errorCount = 0;
        uint currentCounter = startCounter;

        Console.WriteLine($"Processing {gbbIds.Count} Group Billing Bookings in batches of {_config.BatchSize}...");

        // Process in batches
        for (int batchStart = 0; batchStart < gbbIds.Count; batchStart += _config.BatchSize)
        {
            var batchEnd = Math.Min(batchStart + _config.BatchSize, gbbIds.Count);
            var batchIds = gbbIds.GetRange(batchStart, batchEnd - batchStart);
            
            Console.WriteLine($"\n=== Processing batch {batchStart / _config.BatchSize + 1} (bookings {batchStart + 1}-{batchEnd}) ===");
            
            // Fetch all GBBs in this batch
            var batchGBBs = await FetchGroupBillingBookingsBatch(cluster, batchIds);
            
            // Process each GBB in sorted order
            for (int i = 0; i < batchIds.Count; i++)
            {
                try
                {
                    var gbbId = batchIds[i];
                    var globalIndex = batchStart + i + 1;
                    Console.WriteLine($"\n--- Processing GBB {globalIndex}/{gbbIds.Count}: {gbbId} ---");
                    
                    var gbb = batchGBBs.ContainsKey(gbbId) ? batchGBBs[gbbId] : null;
                    
                    if (gbb == null)
                    {
                        LogError(gbbId, new Exception($"GroupBillingBooking {gbbId} not found"));
                        errorCount++;
                        continue;
                    }

                    // Assign new booking ID (increment from current counter)
                    currentCounter++;
                    uint newBookingId = currentCounter;
                    
                    Console.WriteLine($"🔄 Migrating GBB ID: {gbbId} -> Booking ID: {newBookingId}");
                    
                    // Convert GBB to Booking format
                    var booking = ConvertGroupBillingBookingToBooking(gbb, newBookingId);
                    
                    // Insert into Bookings collection
                    await InsertBooking(bucket, newBookingId, booking);
                    
                    // Store mapping
                    _idMapping[gbbId] = newBookingId;
                    
                    successCount++;
                    Console.WriteLine($"✅ Successfully migrated GBB {gbbId} to Booking {newBookingId}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Error processing GBB {batchIds[i]}: {ex.Message}");
                    LogError(batchIds[i], ex);
                    errorCount++;
                }
            }
            
            Console.WriteLine($"=== Completed batch {batchStart / _config.BatchSize + 1} ===");
        }

        return (successCount, errorCount);
    }

    private async Task<Dictionary<string, JObject>> FetchGroupBillingBookingsBatch(ICluster cluster, List<string> gbbIds)
    {
        var gbbs = new Dictionary<string, JObject>();
        
        if (gbbIds.Count == 0)
        {
            return gbbs;
        }

        Console.WriteLine($"Fetching batch of {gbbIds.Count} Group Billing Bookings...");
        
        // Build query with string IDs
        var idsString = string.Join(",", gbbIds.Select(id => $"'{id}'"));
        var query = $"SELECT {_config.SourceCollection}.* FROM `{_config.BucketName}`.`{_config.Scope}`.`{_config.SourceCollection}` WHERE id IN [{idsString}]";
        
        var result = await cluster.QueryAsync<JObject>(query);
        
        await foreach (var row in result)
        {
            if (row != null && row["id"] != null)
            {
                var gbbId = row["id"].ToString();
                gbbs[gbbId] = row;
            }
        }
        
        // Sort the dictionary entries by ID (numerically if possible, otherwise alphabetically)
        var sortedEntries = gbbs.OrderBy(kvp => kvp.Key, Comparer<string>.Create((a, b) =>
        {
            // Try numeric comparison first
            if (uint.TryParse(a, out uint numA) && uint.TryParse(b, out uint numB))
            {
                return numA.CompareTo(numB);
            }
            // Fall back to string comparison
            return string.Compare(a, b, StringComparison.Ordinal);
        })).ToList();
        
        // Rebuild dictionary from sorted entries (maintains insertion order in .NET Core 2.1+)
        var sortedGbbs = new Dictionary<string, JObject>();
        foreach (var kvp in sortedEntries)
        {
            sortedGbbs[kvp.Key] = kvp.Value;
        }
        
        Console.WriteLine($"Successfully fetched {sortedGbbs.Count} Group Billing Bookings from batch");
        return sortedGbbs;
    }

    private JObject ConvertGroupBillingBookingToBooking(JObject gbb, uint newBookingId)
    {
        // Extract GBB fields
        var segmentId = gbb["segmentId"]?.Value<uint>() ?? 0;
        var subSegmentId = gbb["subSegmentId"]?.Value<uint>() ?? 0;
        var groupId = gbb["groupId"]?.ToString() ?? "";
        var date = gbb["date"]?.ToString() ?? "";
        var grossLines = gbb["grossLines"] as JArray ?? new JArray();
        var netLines = gbb["netLines"] as JArray ?? new JArray();
        
        // Calculate totals
        decimal grossTotal = 0;
        foreach (var line in grossLines)
        {
            grossTotal += line[TOTAL] != null ? line[TOTAL].Value<decimal>() : 0;
        }
        grossTotal = Math.Round(grossTotal, 2);
        
        decimal netTotal = 0;
        foreach (var line in netLines)
        {
            netTotal += line[TOTAL] != null ? line[TOTAL].Value<decimal>() : 0;
        }
        netTotal = Math.Round(netTotal, 2);
        
        // Extract metadata from GBB
        var createdBy = gbb["createdBy"]?.ToString() ?? string.Empty;
        var updatedBy = gbb["updatedBy"]?.ToString() ?? string.Empty;
        var createTime = gbb["createTime"]?.Value<DateTime>() ?? DateTime.Now;
        var updateTime = gbb["updateTime"]?.Value<DateTime>() ?? DateTime.Now;
        
        // Create products array - single product with GroupBillingDetails containing all lines
        var products = new JArray();
        
        // Create a single billing product with all gross and net lines
        var billingProduct = CreateBillingProduct(grossLines, netLines, date, createdBy, updatedBy, createTime, updateTime);
        products.Add(billingProduct);
        
        // Create new booking object matching Booking model structure
        var booking = new JObject
        {
            ["id"] = newBookingId,
            ["status"] = 1, // OK status (assuming migrated GBBs are active)
            ["segmentId"] = segmentId,
            ["subSegmentId"] = subSegmentId,
            ["groupId"] = groupId,
            ["products"] = products,
            ["users"] = new JArray(), // Required field - empty for billing-only bookings
            ["passengers"] = new JArray(), // Required field
            ["grossTotal"] = grossTotal,
            ["netTotal"] = netTotal,
            ["clientTotal"] = 0, // For GBB, client total typically equals gross total
            ["grossRoomPrice"] = 0, // Required field - no rooms for billing bookings
            ["netRoomPrice"] = 0, // Required field - no rooms for billing bookings
            ["notes"] = new JArray(),
            ["history"] = new JArray(),
            ["bookingTags"] = new JArray(),
            ["ccPayments"] = 0,
            ["salaryPayments"] = 0,
            ["periodEntitledDays"] = 0,
            ["updatedBy"] = updatedBy,
            ["updateTime"] = updateTime,
            ["createdBy"] = createdBy,
            ["createTime"] = createTime,
            ["externalOrderId"] = "",
            ["period"] = date,
            ["category"] = "",
            ["tmura"] = null,
            ["subsidyComment"] = "",
            ["salary"] = 0
        };
        
        return booking;
    }

    private static JObject CreateBillingProduct(JArray grossLines, JArray netLines, string billingDate, 
        string createdBy, string updatedBy, DateTime createTime, DateTime updateTime)
    {
        // Create GroupBillingDetails
        var productDetails = new JObject
        {
            ["type"] = 3, // Billing type (ProductTypeEnum.Billing)
            ["grossLines"] = grossLines,
            ["netLines"] = netLines,
            ["billingDate"] = billingDate
        };
        
        // Calculate totals from the lines
        decimal grossTotal = grossLines.Sum(line => line[TOTAL]?.Value<decimal>() ?? 0);
        decimal netTotal = netLines.Sum(line => line[TOTAL]?.Value<decimal>() ?? 0);
        
        return new JObject
        {
            ["name"] = "Group Billing",
            ["status"] = 1, // OK status
            ["productDetails"] = productDetails,
            ["grossTotal"] = Math.Round(grossTotal, 2),
            ["netTotal"] = Math.Round(netTotal, 2),
            ["netAdjustment"] = 0,
            ["cancellationDetails"] = null,
            ["updatedBy"] = updatedBy,
            ["updateTime"] = updateTime,
            ["createdBy"] = createdBy,
            ["createTime"] = createTime
        };
    }

    private async Task InsertBooking(IBucket bucket, uint bookingId, JObject booking)
    {
        var scopeObj = await bucket.ScopeAsync(_config.Scope);
        var collection = await scopeObj.CollectionAsync(_config.TargetCollection);
        await collection.UpsertAsync(bookingId.ToString(), booking);
    }

    private async Task SaveIdMapping()
    {
        try
        {
            var fileName = GetLogFilePath(ID_MAPPING_FILE_NAME);
            var mappingJson = Newtonsoft.Json.JsonConvert.SerializeObject(_idMapping, Newtonsoft.Json.Formatting.Indented);
            await File.WriteAllTextAsync(fileName, mappingJson);
            Console.WriteLine($"✅ ID mapping saved to: {fileName}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Failed to save ID mapping: {ex.Message}");
        }
    }

    private async Task UpdateBookingCounter(ICluster cluster, IBucket bucket)
    {
        try
        {
            Console.WriteLine($"\n🔢 Updating ID counter for {_config.TargetCollection}...");
            
            // Query to get the maximum booking ID from the target collection
            var maxIdQuery = $"SELECT MAX(id) as {MAX_ID} FROM `{_config.BucketName}`.`{_config.Scope}`.`{_config.TargetCollection}`";
            var maxIdResult = await cluster.QueryAsync<JObject>(maxIdQuery);
            
            uint maxId = 0;
            await foreach (var row in maxIdResult)
            {
                if (row != null && row[MAX_ID] != null && row[MAX_ID].Type != JTokenType.Null)
                {
                    maxId = row[MAX_ID].Value<uint>();
                }
            }
            
            Console.WriteLine($"📊 Highest booking ID in collection: {maxId}");
            
            // Get current counter value from the target collection
            var scopeObj = await bucket.ScopeAsync(_config.Scope);
            var collection = await scopeObj.CollectionAsync(_config.TargetCollection);
            
            // Try to get the current counter document
            var counterKey = "Counter";
            
            uint currentCounter = 0;
            try
            {
                var counterResult = await collection.GetAsync(counterKey);
                currentCounter = counterResult.ContentAs<uint>();
                Console.WriteLine($"📊 Current counter value: {currentCounter}");
            }
            catch
            {
                Console.WriteLine("📊 No existing counter found, will create new one.");
            }
            
            // Update counter to be equal to the max booking ID
            var newCounterValue = Math.Max(currentCounter, maxId);
            
            if (newCounterValue > currentCounter)
            {
                await collection.UpsertAsync(counterKey, newCounterValue);
                Console.WriteLine($"✅ Updated ID counter from {currentCounter} to {newCounterValue}");
            }
            else
            {
                Console.WriteLine($"✅ ID counter is already up to date (current: {currentCounter}, max booking ID: {maxId})");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Failed to update ID counter: {ex.Message}");
        }
    }

    private void LogError(string gbbId, Exception ex)
    {
        try
        {
            var fileName = GetLogFilePath(ERROR_BOOKINGS_FILE_NAME);
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            
            using (var writer = new StreamWriter(fileName, true))
            {
                writer.WriteLine($"=== ERROR GBB ID: {gbbId} - {timestamp} ===");
                writer.WriteLine($"Error Message: {ex.Message}");
                writer.WriteLine($"Error Type: {ex.GetType().Name}");
                writer.WriteLine($"Stack Trace: {ex.StackTrace}");
                writer.WriteLine();
            }
        }
        catch
        {
            // Ignore logging errors
        }
    }

    private string GetLogFilePath(string fileName)
    {
        if (string.IsNullOrEmpty(_config.LogsDirectory))
        {
            throw new InvalidOperationException("Logging directory not initialized.");
        }
        return Path.Combine(_config.LogsDirectory, fileName);
    }
}

