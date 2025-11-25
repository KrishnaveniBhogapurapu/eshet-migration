using Couchbase;
using Newtonsoft.Json.Linq;

namespace NewBookingMigration;

internal class BookingUpdateMigration
{
    private readonly MigrationConfig _config;
    
    private const string ERROR_BOOKINGS_FILE_NAME = "error_booking_update_report.txt";
    private const string COMPUTED_VALUES_MISMATCH_FILE_NAME = "computed_values_mismatch_report.txt";

    public BookingUpdateMigration(MigrationConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    public async Task RunMigrationAsync()
    {
        try
        {
            Console.WriteLine("Starting Booking model update migration process...");
            Console.WriteLine($"📦 Source Collection: {_config.SourceCollection}");
            Console.WriteLine($"📦 Target Collection: {_config.TargetCollection}");
            
            // Initialize Couchbase connection
            var cluster = await InitializeCouchbaseConnection();
            var bucket = await GetBucket(cluster);
            
            // Get all booking IDs
            var bookingIds = await GetAllBookingIds(cluster);
            
            if (bookingIds.Count == 0)
            {
                Console.WriteLine("No bookings found to update.");
                await cluster.DisposeAsync();
                return;
            }
            
            // Process migration in batches
            var results = await ProcessBookings(cluster, bucket, bookingIds);
            
            // Display final results
            Console.WriteLine($"\n✅ Migration completed!");
            Console.WriteLine($"✅ Successfully updated: {results.successCount}");
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

    private async Task<List<uint>> GetAllBookingIds(ICluster cluster)
    {
        var bookingIds = new List<uint>();
        
        try
        {
            // Get all bookings from target collection with their updateTime and users field
            var targetQuery = $"SELECT id, updateTime, users FROM `{_config.BucketName}`.`{_config.Scope}`.`{_config.TargetCollection}`";
            var migratedBookings = new Dictionary<uint, DateTime>();
            var bookingsWithEmptyUsers = new HashSet<uint>();
            var targetResult = await cluster.QueryAsync<JObject>(targetQuery);
            
            await foreach (var row in targetResult)
            {
                if (row != null && row["id"] != null)
                {
                    var idToken = row["id"];
                    if (idToken != null)
                    {
                        var id = idToken.Value<uint>();
                        DateTime updateTime = DateTime.MinValue;
                        
                        var updateTimeToken = row["updateTime"];
                        if (updateTimeToken != null && updateTimeToken.Type != JTokenType.Null)
                        {
                            if (DateTime.TryParse(updateTimeToken.ToString(), out var parsedTime))
                            {
                                updateTime = parsedTime;
                            }
                        }
                        
                        migratedBookings[id] = updateTime;
                        
                        // Check if users is null or empty
                        var usersToken = row["users"];
                        if (usersToken == null || usersToken.Type == JTokenType.Null)
                        {
                            bookingsWithEmptyUsers.Add(id);
                        }
                        else if (usersToken is JArray usersArray && usersArray.Count == 0)
                        {
                            bookingsWithEmptyUsers.Add(id);
                        }
                    }
                }
            }

            Console.WriteLine($"Found {migratedBookings.Count} migrated bookings in {_config.TargetCollection}");
            Console.WriteLine($"Found {bookingsWithEmptyUsers.Count} bookings with empty or null users field");

            // Get all bookings from source collection with their updateTime
            var sourceQuery = $"SELECT id, updateTime FROM `{_config.BucketName}`.`{_config.Scope}`.`{_config.SourceCollection}`";
            var sourceResult = await cluster.QueryAsync<JObject>(sourceQuery);
            
            int newBookingsCount = 0;
            int updatedBookingsCount = 0;
            int emptyUsersCount = 0;
            
            await foreach (var row in sourceResult)
            {
                if (row != null && row["id"] != null)
                {
                    var idToken = row["id"];
                    if (idToken != null)
                    {
                        var bookingId = idToken.Value<uint>();
                        
                        if (!migratedBookings.ContainsKey(bookingId))
                        {
                            // New booking - needs insertion
                            bookingIds.Add(bookingId);
                            newBookingsCount++;
                        }
                        else
                        {
                            // Existing booking - check if source version is newer
                            DateTime sourceUpdateTime = DateTime.MinValue;
                            
                            var updateTimeToken = row["updateTime"];
                            if (updateTimeToken != null && updateTimeToken.Type != JTokenType.Null)
                            {
                                if (DateTime.TryParse(updateTimeToken.ToString(), out var parsedTime))
                                {
                                    sourceUpdateTime = parsedTime;
                                }
                            }
                            
                            var targetUpdateTime = migratedBookings[bookingId];
                            if (sourceUpdateTime > targetUpdateTime)
                            {
                                Console.WriteLine($"📝 Booking {bookingId} needs update (source: {sourceUpdateTime:yyyy-MM-dd HH:mm:ss}, target: {targetUpdateTime:yyyy-MM-dd HH:mm:ss})");
                                bookingIds.Add(bookingId);
                                updatedBookingsCount++;
                            }
                        }
                    }
                }
            }
            
            // Add bookings from target collection that have empty or null users
            foreach (var bookingId in bookingsWithEmptyUsers)
            {
                if (!bookingIds.Contains(bookingId))
                {
                    Console.WriteLine($"📝 Booking {bookingId} needs update (empty or null users field)");
                    bookingIds.Add(bookingId);
                    emptyUsersCount++;
                }
            }
            
            Console.WriteLine($"Found {newBookingsCount} new bookings, {updatedBookingsCount} updated bookings, and {emptyUsersCount} bookings with empty/null users to process (total: {bookingIds.Count})");            
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Failed to fetch Booking IDs: {ex.Message}");
        }
        
        return bookingIds;
    }

    private async Task<(int successCount, int errorCount)> ProcessBookings(
        ICluster cluster, IBucket bucket, List<uint> bookingIds)
    {
        int successCount = 0;
        int errorCount = 0;

        Console.WriteLine($"Processing {bookingIds.Count} bookings in batches of {_config.BatchSize}...");

        // Process in batches
        for (int batchStart = 0; batchStart < bookingIds.Count; batchStart += _config.BatchSize)
        {
            var batchEnd = Math.Min(batchStart + _config.BatchSize, bookingIds.Count);
            var batchIds = bookingIds.GetRange(batchStart, batchEnd - batchStart);
            
            Console.WriteLine($"\n=== Processing batch {batchStart / _config.BatchSize + 1} (bookings {batchStart + 1}-{batchEnd}) ===");
            
            // Fetch all bookings in this batch
            var batchBookings = await FetchBookingsBatch(cluster, batchIds);
            
            // Process each booking
            for (int i = 0; i < batchIds.Count; i++)
            {
                try
                {
                    var bookingId = batchIds[i];
                    var globalIndex = batchStart + i + 1;
                    Console.WriteLine($"\n--- Processing Booking {globalIndex}/{bookingIds.Count}: {bookingId} ---");
                    
                    var booking = batchBookings.ContainsKey(bookingId) ? batchBookings[bookingId] : null;
                    
                    if (booking == null)
                    {
                        LogError(bookingId.ToString(), new Exception($"Booking {bookingId} not found"));
                        errorCount++;
                        continue;
                    }

                    // Update booking to new model structure
                    var (updatedBooking, hasMismatches) = UpdateBookingToNewModel(booking, bookingId);
                    
                    // Log computed value mismatches if any
                    if (hasMismatches)
                    {
                        Console.WriteLine($"  ⚠️  Computed value mismatches detected - check log file");
                    }
                    
                    // Update in Couchbase
                    await UpdateBooking(bucket, bookingId, updatedBooking);
                    
                    successCount++;
                    Console.WriteLine($"✅ Successfully updated Booking {bookingId}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Error processing Booking {batchIds[i]}: {ex.Message}");
                    LogError(batchIds[i].ToString(), ex);
                    errorCount++;
                }
            }
            
            Console.WriteLine($"=== Completed batch {batchStart / _config.BatchSize + 1} ===");
        }

        return (successCount, errorCount);
    }

    private async Task<Dictionary<uint, JObject>> FetchBookingsBatch(ICluster cluster, List<uint> bookingIds)
    {
        var bookings = new Dictionary<uint, JObject>();
        
        if (bookingIds.Count == 0)
        {
            return bookings;
        }

        Console.WriteLine($"Fetching batch of {bookingIds.Count} bookings...");
        
        // Build query with numeric IDs
        var idsString = string.Join(",", bookingIds);
        var query = $"SELECT {_config.SourceCollection}.* FROM `{_config.BucketName}`.`{_config.Scope}`.`{_config.SourceCollection}` WHERE id IN [{idsString}]";
        
        var result = await cluster.QueryAsync<JObject>(query);
        
        await foreach (var row in result)
        {
            if (row != null && row["id"] != null)
            {
                var bookingId = row["id"].Value<uint>();
                bookings[bookingId] = row;
            }
        }
        
        Console.WriteLine($"Successfully fetched {bookings.Count} bookings from batch");
        return bookings;
    }

    private (JObject booking, bool hasMismatches) UpdateBookingToNewModel(JObject booking, uint bookingId)
    {
        bool hasMismatches = false;
        
        // Extract old model fields
        var ownerKey = booking["ownerKey"]?.ToString() ?? string.Empty;
        
        // If no ownerKey, skip (shouldn't happen since we filter in query, but safety check)
        if (string.IsNullOrEmpty(ownerKey))
        {
            Console.WriteLine($" ** Booking has no ownerKey");
            if (booking["tmura"] != null && booking["tmura"] is JObject tmura)
            {
                Console.WriteLine($" ** Migrating from tmura");
                tmura["selfPaymentTotal"] = booking["clientPrice"]?["extras"]?.Sum(extra => extra?["extra"]?.Value<decimal>() ?? 0m) ?? 0m;
            }
            else
            {
                Console.WriteLine($" ** Booking has no tmura");
            }
            EnsureNewFieldsExist(booking);
            return (booking, false);
        }
        
        var clientPrice = booking["clientPrice"] as JObject;
        var specialRequests = booking["specialRequests"] as JArray ?? new JArray();
        var requests = booking["requests"] as JArray ?? new JArray();
        var allRequests = new JArray(requests.Concat(specialRequests));
        var requestSections = new JObject();
        if (allRequests.Count > 0)
        {
            requestSections["requests"] = allRequests;
        }
        
        Console.WriteLine($"  🔄 Migrating from old model structure...");
        Console.WriteLine($"     OwnerKey: {ownerKey}");
        
        // Create Users array with default user
        var users = new JArray();
        var user = new JObject
        {
            ["key"] = ownerKey,
            ["confirmed"] = true,
            ["basePrice"] = 0,
            ["paymentType"] = 0, // PaymentType.Salary
            ["clientTotal"] = 0,
            ["requests"] = allRequests,
            ["specialRequests"] = allRequests,
            ["requestSections"] = requestSections,
            ["clientPrice"] = clientPrice ?? CreateDefaultClientPrice()
        };
        
        users.Add(user);
        booking["users"] = users;
        
        // Remove old fields from booking root
        booking.Remove("ownerKey");
        booking.Remove("clientPrice");
        booking.Remove("specialRequests");
        
        // Ensure new fields exist with defaults
        EnsureNewFieldsExist(booking);
        
        // Check and update computed values
        hasMismatches = CheckAndUpdateComputedValues(booking, user, bookingId);
        
        Console.WriteLine($"  ✅ Created Users array with 1 user (Key: {ownerKey})");
        
        return (booking, hasMismatches);
    }

    private bool CheckAndUpdateComputedValues(JObject booking, JObject user, uint bookingId)
    {
        bool hasMismatches = false;
        var mismatches = new List<string>();
        
        // Calculate computed GrossTotal
        decimal? computedGrossTotal = CalculateGrossTotal(booking);
        decimal? storedGrossTotal = booking["grossTotal"]?.Value<decimal?>();
        
        if (computedGrossTotal.HasValue && storedGrossTotal.HasValue)
        {
            if (Math.Abs(computedGrossTotal.Value - storedGrossTotal.Value) > 0.01m)
            {
                hasMismatches = true;
                mismatches.Add($"GrossTotal: stored={storedGrossTotal.Value}, computed={computedGrossTotal.Value}");
                booking["grossTotal"] = computedGrossTotal.Value;
            }
        }
        else if (computedGrossTotal.HasValue && !storedGrossTotal.HasValue)
        {
            booking["grossTotal"] = computedGrossTotal.Value;
        }
        
        // Calculate computed NetTotal
        decimal? computedNetTotal = CalculateNetTotal(booking);
        decimal? storedNetTotal = booking["netTotal"]?.Value<decimal?>();
        
        if (computedNetTotal.HasValue && storedNetTotal.HasValue)
        {
            if (Math.Abs(computedNetTotal.Value - storedNetTotal.Value) > 0.01m)
            {
                hasMismatches = true;
                mismatches.Add($"NetTotal: stored={storedNetTotal.Value}, computed={computedNetTotal.Value}");
                booking["netTotal"] = computedNetTotal.Value;
            }
        }
        else if (computedNetTotal.HasValue && !storedNetTotal.HasValue)
        {
            booking["netTotal"] = computedNetTotal.Value;
        }
        
        // Calculate computed ClientTotal
        decimal computedClientTotal = CalculateClientTotal(booking, user);
        decimal? storedClientTotal = booking["clientTotal"]?.Value<decimal?>();
        
        if (storedClientTotal.HasValue)
        {
            if (Math.Abs(computedClientTotal - storedClientTotal.Value) > 0.01m)
            {
                hasMismatches = true;
                mismatches.Add($"ClientTotal: stored={storedClientTotal.Value}, computed={computedClientTotal}");
                booking["clientTotal"] = computedClientTotal;
            }
        }
        else
        {
            booking["clientTotal"] = computedClientTotal;
        }
        
        // Log mismatches if any
        if (hasMismatches)
        {
            LogComputedValueMismatch(bookingId, mismatches);
        }
        
        return hasMismatches;
    }

    private decimal? CalculateGrossTotal(JObject booking)
    {
        decimal total = 0;
        var products = booking["products"] as JArray;

        // Sum from products
        if (products != null)
        {
            foreach (var product in products)
            {
                var productGrossTotal = product["grossTotal"]?.Value<decimal?>();
                if (productGrossTotal.HasValue)
                {
                    total += productGrossTotal.Value;
                }
            }
        }
        
        return Math.Round(total, 2);
    }

    private decimal? CalculateNetTotal(JObject booking)
    {
        decimal total = 0;
        var products = booking["products"] as JArray;
        
        if (products != null)
        {
            foreach (var product in products)
            {
                var productNetTotal = product["netTotal"]?.Value<decimal?>();
                if (productNetTotal.HasValue)
                {
                    total += productNetTotal.Value;
                }
            }
        }
        
        return Math.Round(total, 2);
    }

    private decimal CalculateClientTotal(JObject booking, JObject user)
    {   
            // For regular bookings: use first user's ClientPrice
            var clientPrice = user["clientPrice"] as JObject;
            if (clientPrice == null) return 0;
            
            var extras = clientPrice["extras"] as JArray ?? new JArray();
            var subsidies = clientPrice["subsidies"] as JArray ?? new JArray();
            
            decimal extrasSum = extras.Sum(e => e["extra"]?.Value<decimal>() ?? 0m);
            decimal subsidiesSum = subsidies.Sum(s => s["extra"]?.Value<decimal>() ?? 0m);
            
            decimal client = extrasSum - subsidiesSum;
            
            // Add interest from InterestDetails products
            decimal interest = CalculateInterest(booking);
            client += interest;
            
            client = Math.Round(client, 2);
            
            var grossTotal = booking["grossTotal"]?.Value<decimal?>();
            if (grossTotal.HasValue && client > grossTotal.Value) return grossTotal.Value;
            if (client < 0) return 0;
            return client;

    }

    private decimal CalculateInterest(JObject booking)
    {
        decimal interest = 0;
        var products = booking["products"] as JArray;
        
        if (products != null)
        {
            foreach (var product in products)
            {
                var productDetails = product["productDetails"] as JObject;
                var type = productDetails?["type"];
                
                // InterestDetails type = 2 (ProductTypeEnum.Interest)
                if (type != null && type.Value<int>() == 2)
                {
                    var productInterest = productDetails?["interest"]?.Value<decimal>() ?? 0m;
                    interest += productInterest;
                }
            }
        }
        
        return interest;
    }

    private void LogComputedValueMismatch(uint bookingId, List<string> mismatches)
    {
        try
        {
            var fileName = GetLogFilePath(COMPUTED_VALUES_MISMATCH_FILE_NAME);
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            
            using (var writer = new StreamWriter(fileName, true))
            {
                writer.WriteLine($"=== Booking ID: {bookingId} - {timestamp} ===");
                foreach (var mismatch in mismatches)
                {
                    writer.WriteLine($"  {mismatch}");
                }
                writer.WriteLine();
            }
        }
        catch
        {
            // Ignore logging errors
        }
    }

    private static void EnsureNewFieldsExist(JObject booking)
    {
        // Add new fields if they don't exist
        if (booking["passengers"] == null)
        {
            booking["passengers"] = new JArray();
        }
        
        if (booking["grossRoomPrice"] == null)
        {
            booking["grossRoomPrice"] = 0;
        }
        
        if (booking["netRoomPrice"] == null)
        {
            booking["netRoomPrice"] = 0;
        }

        if (booking["groupId"] == null)
        {
            booking["groupId"] = string.Empty;
        }

        if (booking["bookingTags"] == null)
        {
            booking["bookingTags"] = new JArray();
        }

        if (booking["category"] == null)
        {
            booking["category"] = string.Empty;
        }

        if (booking["salary"] == null)
        {
            booking["salary"] = 0;
        }

        if (booking["salaryPayments"] == null)
        {
            booking["salaryPayments"] = 0;
        }

        if (booking["subsidyComment"] == null)
        {
            booking["subsidyComment"] = string.Empty;
        }

        var hotelDetails = booking["products"]?.FirstOrDefault(p => p["productDetails"]?["type"]?.Value<int>() == 0)?["productDetails"] as JObject;
        if (hotelDetails != null)
        {
            if (hotelDetails["start"] == null)
            {
                hotelDetails["start"] = new DateOnly().ToString("yyyy-MM-dd");
            }
            if (hotelDetails["end"] == null)
            {
                hotelDetails["end"] = new DateOnly().ToString("yyyy-MM-dd");
            }
            if (hotelDetails["roomLabel"] == null)
            {
                hotelDetails["roomLabel"] = string.Empty;
            }
        }
    }

    private static JObject CreateDefaultClientPrice()
    {
        return new JObject
        {
            ["entitledPax"] = new JArray(),
            ["extras"] = new JArray(),
            ["subsidies"] = new JArray(),
            ["entitledStay"] = 0
        };
    }

    private async Task UpdateBooking(IBucket bucket, uint bookingId, JObject booking)
    {
        var scopeObj = await bucket.ScopeAsync(_config.Scope);
        var collection = await scopeObj.CollectionAsync(_config.TargetCollection);
        await collection.UpsertAsync(bookingId.ToString(), booking);
    }

    private void LogError(string bookingId, Exception ex)
    {
        try
        {
            var fileName = GetLogFilePath(ERROR_BOOKINGS_FILE_NAME);
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            
            using (var writer = new StreamWriter(fileName, true))
            {
                writer.WriteLine($"=== ERROR Booking ID: {bookingId} - {timestamp} ===");
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

