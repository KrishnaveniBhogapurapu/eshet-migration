using System.Linq;
using Couchbase;
using Couchbase.KeyValue;
using Couchbase.Query;
using Newtonsoft.Json.Linq;

namespace NewBookingMigration;

internal class GroupBillingBookingMigration
{
    private readonly MigrationConfig _config;

    private const string ERROR_BOOKINGS_FILE_NAME = "error_gbb_migration_report.txt";
    private const string TOTAL = "total";

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
            var results = await ProcessGroupBillingBookings(cluster, bucket, gbbIds);
            
            // Display final results
            Console.WriteLine($"\n✅ Migration completed!");
            Console.WriteLine($"✅ Successfully migrated: {results.successCount}");
            Console.WriteLine($"❌ Errors: {results.errorCount}");
            
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

    private async Task<List<string>> GetAllGroupBillingBookingIds(ICluster cluster)
    {
        var gbbIds = new List<string>();
        
        try
        {
            // Get all bookings from target collection with their updateTime (those with billing products)
            var targetQuery = $@"
                SELECT id, updateTime 
                FROM `{_config.BucketName}`.`{_config.Scope}`.`{_config.TargetCollection}`
                WHERE ANY p IN products SATISFIES p.productDetails.type = 3 END";
            var migratedBookings = new Dictionary<string, DateTime>(); // GBB ID (as string) -> updateTime
            var targetResult = await cluster.QueryAsync<JObject>(targetQuery);
            
            await foreach (var row in targetResult)
            {
                if (row != null && row["id"] != null)
                {
                    var idToken = row["id"];
                    if (idToken != null)
                    {
                        // Booking ID in target is uint, but we'll use it as string to match with GBB ID
                        var bookingId = idToken.ToString();
                        DateTime updateTime = DateTime.MinValue;
                        
                        var updateTimeToken = row["updateTime"];
                        if (updateTimeToken != null && updateTimeToken.Type != JTokenType.Null)
                        {
                            if (DateTime.TryParse(updateTimeToken.ToString(), out var parsedTime))
                            {
                                updateTime = parsedTime;
                            }
                        }
                        
                        migratedBookings[bookingId] = updateTime;
                    }
                }
            }

            Console.WriteLine($"Found {migratedBookings.Count} migrated Group Billing Bookings in {_config.TargetCollection}");

            // Get all Group Billing Bookings from source collection with their updateTime
            var sourceQuery = $"SELECT id, updateTime FROM `{_config.BucketName}`.`{_config.Scope}`.`{_config.SourceCollection}`";
            var sourceResult = await cluster.QueryAsync<JObject>(sourceQuery);
            
            int newGBBsCount = 0;
            int updatedGBBsCount = 0;
            
            await foreach (var row in sourceResult)
            {
                if (row != null && row["id"] != null)
                {
                    var idToken = row["id"];
                    if (idToken != null)
                    {
                        var gbbId = idToken.ToString();
                        
                        if (!migratedBookings.ContainsKey(gbbId))
                        {
                            // New GBB - needs migration
                            gbbIds.Add(gbbId);
                            newGBBsCount++;
                        }
                        else
                        {
                            // Existing GBB - check if source version is newer
                            DateTime sourceUpdateTime = DateTime.MinValue;
                            
                            var updateTimeToken = row["updateTime"];
                            if (updateTimeToken != null && updateTimeToken.Type != JTokenType.Null)
                            {
                                if (DateTime.TryParse(updateTimeToken.ToString(), out var parsedTime))
                                {
                                    sourceUpdateTime = parsedTime;
                                }
                            }
                            
                            var targetUpdateTime = migratedBookings[gbbId];
                            if (sourceUpdateTime > targetUpdateTime)
                            {
                                Console.WriteLine($"📝 Group Billing Booking {gbbId} needs update (source: {sourceUpdateTime:yyyy-MM-dd HH:mm:ss}, target: {targetUpdateTime:yyyy-MM-dd HH:mm:ss})");
                                gbbIds.Add(gbbId);
                                updatedGBBsCount++;
                            }
                        }
                    }
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
            
            Console.WriteLine($"Found {newGBBsCount} new Group Billing Bookings and {updatedGBBsCount} updated Group Billing Bookings to process (total: {gbbIds.Count})");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Failed to fetch Group Billing Booking IDs: {ex.Message}");
        }
        
        return gbbIds;
    }

    private async Task<(int successCount, int errorCount)> ProcessGroupBillingBookings(
        ICluster cluster, IBucket bucket, List<string> gbbIds)
    {
        int successCount = 0;
        int errorCount = 0;

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

                    // Parse GBB ID to uint for booking ID (use same ID)
                    if (!uint.TryParse(gbbId, out uint bookingId))
                    {
                        LogError(gbbId, new Exception($"GroupBillingBooking ID '{gbbId}' cannot be parsed as uint"));
                        errorCount++;
                        continue;
                    }
                    
                    Console.WriteLine($"🔄 Migrating GBB ID: {gbbId} -> Booking ID: {bookingId}");
                    
                    // Convert GBB to Booking format
                    var booking = ConvertGroupBillingBookingToBooking(gbb, bookingId);
                    
                    // Insert/Update into Bookings collection
                    await InsertBooking(bucket, bookingId, booking);
                    
                    successCount++;
                    Console.WriteLine($"✅ Successfully migrated GBB {gbbId} to Booking {bookingId}");
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

    private JObject ConvertGroupBillingBookingToBooking(JObject gbb, uint bookingId)
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
            ["id"] = bookingId,
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

