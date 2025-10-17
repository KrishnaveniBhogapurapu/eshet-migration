using Couchbase;
using Couchbase.KeyValue;
using Couchbase.Query;
using Newtonsoft.Json.Linq;

class Program
{
    private const string CONNECTION_STRING = "vm2.server-6snuprr7isbki.northeurope.cloudapp.azure.com";
    private const string USERNAME = "";
    private const string PASSWORD = "";
    private const string BUCKET_NAME = "Apollo";
    private const string SCOPE = "Eshet";
    private const string OLD_COLLECTION = "Bookings";
    private const string NEW_COLLECTION = "Booking3";
    // Configuration constants
    private const int BATCH_SIZE = 50; // Process 50 bookings at a time
    private const string PROGRESS_FILE = "migration_progress.json";
    private const string SKIPPED_BOOKINGS_FILE = "skipped_bookings_report.txt";
    private const string IGNORED_FIELDS_FILE = "ignored_fields_report.txt";
    private const string ERROR_BOOKINGS_FILE = "error_bookings_report.txt";
    private const int BATCH_DELAY_MS = 100; // Delay between batches

    static async Task Main(string[] args)
    {
        try
        {
            Console.WriteLine("Starting booking migration process...");
            
            // Initialize Couchbase connection
            var cluster = await InitializeCouchbaseConnection();
            var bucket = await GetBucket(cluster);
            
            // Get total count for progress tracking
            var totalBookings = await GetTotalBookingCount(cluster);
            Console.WriteLine($"Total bookings to migrate: {totalBookings}");
            
            if (totalBookings == 0)
            {
                Console.WriteLine("No bookings found to migrate.");
                return;
            }
            
            // Check for existing progress
            var progress = LoadProgress();
            var startOffset = progress?.LastProcessedOffset + 1 ?? 0;
            
            if (startOffset > 0)
            {
                Console.WriteLine($"Resuming migration from offset {startOffset}...");
            }
            
            // Process bookings in batches
            var migrationResults = await ProcessBookingsInBatches(cluster, bucket, totalBookings, startOffset, progress);
            
            // Display final results
            Console.WriteLine($"\nMigration completed!");
            Console.WriteLine($"✅ Success: {migrationResults.successCount}");
            Console.WriteLine($"❌ Errors: {migrationResults.errorCount}");
            Console.WriteLine($"⚠️  Skipped (totals mismatch): {migrationResults.skippedCount}");
            
            // Clear progress file only on successful completion with no errors
            if (migrationResults.errorCount == 0)
            {
                // ClearProgress();
                Console.WriteLine("Progress file cleared - migration completed successfully!");
            }
            else
            {
                Console.WriteLine($"Progress file preserved - {migrationResults.errorCount} errors occurred. You can resume the migration.");
            }
            
            // Cleanup
            await cluster.DisposeAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Migration failed: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
        }
    }
    private static async Task<ICluster> InitializeCouchbaseConnection()
    {
        Console.WriteLine("Connecting to Couchbase...");
        
        var cluster = await Cluster.ConnectAsync(CONNECTION_STRING, options =>
        {
            options.UserName = USERNAME;
            options.Password = PASSWORD;
            options.EnableTls = true;
            options.EnableDnsSrvResolution = true;
            options.KvCertificateCallbackValidation = (sender, certificate, chain, sslPolicyErrors) => true;
            options.HttpCertificateCallbackValidation = (sender, certificate, chain, sslPolicyErrors) => true;
        });
        
        Console.WriteLine("Connected to Couchbase successfully!");
        return cluster;
    }

    private static async Task<IBucket> GetBucket(ICluster cluster)
    {
        var bucket = await cluster.BucketAsync(BUCKET_NAME);
        await bucket.WaitUntilReadyAsync(TimeSpan.FromSeconds(10));
        return bucket;
    }
    private static async Task<List<JObject>> FetchBookings(ICluster cluster, int offset = 0, int limit = 100)
    {
        Console.WriteLine($"Fetching bookings from Bookings table (offset: {offset}, limit: {limit})...");
        
        var query = $"SELECT {OLD_COLLECTION}.* FROM `{BUCKET_NAME}`.`{SCOPE}`.`{OLD_COLLECTION}` LIMIT {limit} OFFSET {offset}";
        var result = await cluster.QueryAsync<JObject>(query);
        
        var bookings = new List<JObject>();
        await foreach (var row in result)
        {
            if (row != null && row["atlantisHotelId"] != null)
            {
                bookings.Add(row);
            }
        }
        
        Console.WriteLine($"Found {bookings.Count} bookings in this batch");
        return bookings;
    }

    private static async Task<int> GetTotalBookingCount(ICluster cluster)
    {
        Console.WriteLine("Getting total booking count...");
        
        var query = $"SELECT COUNT(*) as total FROM `{BUCKET_NAME}`.`{SCOPE}`.`{OLD_COLLECTION}` WHERE atlantisHotelId IS NOT NULL";
        var result = await cluster.QueryAsync<JObject>(query);
        
        await foreach (var row in result)
        {
            return row["total"]?.Value<int>() ?? 0;
        }
        
        return 0;
    }

    private static async Task<(int successCount, int errorCount, int skippedCount)> ProcessBookingsInBatches(ICluster cluster, IBucket bucket, int totalBookings, int startOffset = 0, MigrationProgress? existingProgress = null)
    {
        int totalSuccessCount = existingProgress?.TotalSuccessCount ?? 0;
        int totalErrorCount = existingProgress?.TotalErrorCount ?? 0;
        int totalSkippedCount = existingProgress?.TotalSkippedCount ?? 0;
        int processedCount = startOffset;

        Console.WriteLine($"Processing {totalBookings} bookings in batches of {BATCH_SIZE}...");
        if (startOffset > 0)
        {
            Console.WriteLine($"Resuming from offset {startOffset} (already processed: {totalSuccessCount} success, {totalErrorCount} errors, {totalSkippedCount} skipped)");
        }

        var progress = existingProgress ?? new MigrationProgress();
        var startTime = System.Diagnostics.Stopwatch.StartNew();

        for (int offset = startOffset; offset < totalBookings; offset += BATCH_SIZE)
        {
            try
            {
                Console.WriteLine($"\n--- Processing batch {offset / BATCH_SIZE + 1} (offset: {offset}) ---");
                
                // Fetch current batch
                var bookings = await FetchBookings(cluster, offset, BATCH_SIZE);
                
                if (bookings.Count == 0)
                {
                    Console.WriteLine("No more bookings to process.");
                    break;
                }

                // Process current batch
                var batchResults = await ProcessBookingsBatch(bucket, bookings);
                
                // Update totals
                totalSuccessCount += batchResults.successCount;
                totalErrorCount += batchResults.errorCount;
                totalSkippedCount += batchResults.skippedCount;
                processedCount += bookings.Count;

                // Progress update with performance metrics
                var progressPercentage = (double)processedCount / totalBookings * 100;
                var elapsed = startTime.Elapsed;
                var bookingsPerSecond = processedCount / elapsed.TotalSeconds;
                var estimatedTimeRemaining = TimeSpan.FromSeconds((totalBookings - processedCount) / bookingsPerSecond);
                
                Console.WriteLine($"Progress: {processedCount}/{totalBookings} ({progressPercentage:F1}%)");
                Console.WriteLine($"Performance: {bookingsPerSecond:F1} bookings/sec | Elapsed: {elapsed:hh\\:mm\\:ss} | ETA: {estimatedTimeRemaining:hh\\:mm\\:ss}");
                Console.WriteLine($"Batch results - Success: {batchResults.successCount}, Errors: {batchResults.errorCount}, Skipped: {batchResults.skippedCount}");

                // Update and save progress
                progress.LastProcessedOffset = offset + bookings.Count - 1;
                progress.TotalSuccessCount = totalSuccessCount;
                progress.TotalErrorCount = totalErrorCount;
                progress.TotalSkippedCount = totalSkippedCount;
                progress.LastUpdated = DateTime.Now;
                SaveProgress(progress);

                // Add small delay between batches to prevent overwhelming the database
                if (offset + BATCH_SIZE < totalBookings)
                {
                    await Task.Delay(BATCH_DELAY_MS);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error processing batch at offset {offset}: {ex.Message}");
                totalErrorCount += BATCH_SIZE; // Assume all in this batch failed
                processedCount += BATCH_SIZE;
            }
        }

        return (totalSuccessCount, totalErrorCount, totalSkippedCount);
    }

    private static async Task<(int successCount, int errorCount, int skippedCount)> ProcessBookingsBatch(IBucket bucket, List<JObject> bookings)
    {
        int successCount = 0;
        int errorCount = 0;
        int skippedCount = 0;

        foreach (var oldBooking in bookings)
        {
            try
            {
                var bookingId = oldBooking["id"]?.ToString() ?? "unknown";
                Console.WriteLine($"Processing booking ID: {bookingId}");
                
                // Convert to new booking format and validate totals
                var (newBooking, shouldInsert) = ConvertToNewBooking(oldBooking);
                
                if (!shouldInsert)
                {
                    skippedCount++;
                    continue;
                }
                
                // Insert into new collection only if totals match
                await InsertBookingToNewCollection(bucket, oldBooking, newBooking);
                
                successCount++;
                Console.WriteLine($"✅ Successfully migrated booking ID: {bookingId}");
            }
            catch (Exception ex)
            {
                errorCount++;
                var bookingId = oldBooking["id"]?.ToString() ?? "unknown";
                Console.WriteLine($"❌ Failed to migrate booking ID: {bookingId} - {ex.Message}");
                
                // Log error booking with full details
                LogErrorBooking(oldBooking, ex);
            }
        }

        return (successCount, errorCount, skippedCount);
    }

    private static async Task InsertBookingToNewCollection(IBucket bucket, JObject oldBooking, JObject newBooking)
    {
        var scopeObj = await bucket.ScopeAsync(SCOPE);
        var collection = await scopeObj.CollectionAsync(NEW_COLLECTION);
        var key = oldBooking["id"]?.ToString();
        await collection.UpsertAsync(key, newBooking);
    }


    private static (JObject newBooking, bool shouldInsert) ConvertToNewBooking(JObject oldBooking)
    {
        // Create products array
        var products = new JArray();

        // Create hotel product
        var hotelProduct = CreateHotelProduct(oldBooking);
        products.Add(hotelProduct);

        // Create interest product (if interest exists)
        var interest = oldBooking["interest"] != null ? oldBooking["interest"].Value<decimal>() : 0;
        if (interest > 0)
        {
            var interestProduct = CreateInterestProduct(oldBooking, interest);
            products.Add(interestProduct);
        }

        // Create service products (if services exist)
        var services = oldBooking["services"] as JArray ?? new JArray();
        foreach (var service in services)
        {
            var serviceProduct = CreateServiceProduct(oldBooking, service);
            products.Add(serviceProduct);
        }

        // Calculate booking-level totals
        var (bookingGrossTotal, bookingNetTotal, clientTotal) = CalculateBookingTotals(products, oldBooking, interest);

        // Create new booking object
        var newBooking = CreateNewBookingObject(oldBooking, products, bookingGrossTotal, bookingNetTotal, clientTotal);

        // Validate totals and determine if migration should proceed
        var shouldInsert = ValidateAndDetermineMigration(oldBooking, newBooking);
        
        return (newBooking, shouldInsert);
    }

    private static JObject CreateHotelProduct(JObject oldBooking)
    {
        var board = oldBooking["board"]?.Value<string>() ?? "";
        var clientComment = oldBooking["clientComment"]?.Value<string>() ?? "";
        var hotelComment = oldBooking["hotelComment"]?.Value<string>() ?? "";
        var rooms = oldBooking["rooms"] as JArray ?? new JArray();
        var status = oldBooking["status"] != null ? oldBooking["status"].Value<int>() : 0;
        
        // Calculate hotel product totals (following Product.CalculateGrossTotal logic)
        decimal hotelGrossTotal = 0;
        decimal hotelNetTotal = 0;
        
        if (status == 2) // CANCELLED status
        {
            // For cancelled bookings, use cancellation fees instead of room totals
            hotelGrossTotal = Math.Round(oldBooking["cancellationFeeGross"] != null ? oldBooking["cancellationFeeGross"].Value<decimal>() : 0, 2);
            hotelNetTotal = Math.Round(oldBooking["cancellationFeeNet"] != null ? oldBooking["cancellationFeeNet"].Value<decimal>() : 0, 2);
        }
        else
        {
            // For non-cancelled bookings, sum room totals
            foreach (var room in rooms)
            {
                hotelGrossTotal += room["gross"] != null ? room["gross"].Value<decimal>() : 0;
                hotelNetTotal += room["net"] != null ? room["net"].Value<decimal>() : 0;
                if (board != "") {
                    room["board"] = board;
                }
                if (clientComment != "") {
                    room["clientComment"] = clientComment;
                }
                if (hotelComment != "") {
                    room["hotelComment"] = hotelComment;
                }
            }
            hotelGrossTotal = Math.Round(hotelGrossTotal, 2);
            hotelNetTotal = Math.Round(hotelNetTotal, 2);
        }

        return new JObject
        {
            ["name"] = "Hotel Booking",
            ["status"] = oldBooking["status"],
            ["productDetails"] = new JObject
            {
                ["type"] = 0, // Hotel type
                ["apollo"] = oldBooking["apollo"],
                ["atlantisHotelId"] = oldBooking["atlantisHotelId"],
                ["bookingIdFromHotel"] = oldBooking["hotelBookingId"] ?? "",
                ["hotelSegmentId"] = oldBooking["hotelSegmentId"],
                ["rooms"] = rooms
            },
            ["cancellationDetails"] = oldBooking["status"]?.Value<int>() == 2 ? new JObject
            {
                ["cancellationFeeNet"] = oldBooking["cancellationFeeNet"] ?? 0,
                ["cancellationFeeGross"] = oldBooking["cancellationFeeGross"] ?? 0,
                ["cancelledTime"] = oldBooking["cancelledTime"],
                ["cancellationReason"] = oldBooking["cancellationReason"] ?? ""
            } : null,
            ["netAdjustment"] = oldBooking["netAdjustment"] ?? 0,
            ["updatedBy"] = oldBooking["updatedBy"],
            ["updateTime"] = oldBooking["updateTime"],
            ["createdBy"] = oldBooking["createdBy"],
            ["createTime"] = oldBooking["createTime"],
            ["grossTotal"] = hotelGrossTotal,
            ["netTotal"] = hotelNetTotal
        };
    }

    private static JObject CreateInterestProduct(JObject oldBooking, decimal interest)
    {
        var status = oldBooking["status"] != null ? oldBooking["status"].Value<int>() : 0;
        
        // For interest products, both gross and net totals are the interest amount (following Product.CalculateGrossTotal logic)
        // Note: Interest products don't have cancellation fees, so they remain the same even when cancelled
        var interestGrossTotal = Math.Round(interest, 2);
        var interestNetTotal = Math.Round(interest, 2);

        return new JObject
        {
            ["name"] = "Interest",
            ["status"] = oldBooking["status"],
            ["productDetails"] = new JObject
            {
                ["type"] = 2, // Interest type
                ["interest"] = interest
            },
            ["netAdjustment"] = 0,
            ["updatedBy"] = oldBooking["updatedBy"],
            ["updateTime"] = oldBooking["updateTime"],
            ["createdBy"] = oldBooking["createdBy"],
            ["createTime"] = oldBooking["createTime"],
            ["grossTotal"] = status == 2 ? 0 : interestGrossTotal,
            ["netTotal"] = status == 2 ? 0 : interestNetTotal
        };
    }

    private static JObject CreateServiceProduct(JObject oldBooking, JToken service)
    {
        var status = oldBooking["status"] != null ? oldBooking["status"].Value<int>() : 0;
        
        // For service products, gross and net totals come from the service object (following Product.CalculateGrossTotal logic)
        // Note: Service products don't have cancellation fees, so they remain the same even when cancelled
        var serviceGrossTotal = Math.Round(service["gross"] != null ? service["gross"].Value<decimal>() : 0, 2);
        var serviceNetTotal = Math.Round(service["net"] != null ? service["net"].Value<decimal>() : 0, 2);

        return new JObject
        {
            ["name"] = service["name"]?.Value<string>() ?? "Service",
            ["status"] = oldBooking["status"],
            ["productDetails"] = new JObject
            {
                ["type"] = 1, // Service type
                ["service"] = service
            },
            ["netAdjustment"] = 0,
            ["updatedBy"] = oldBooking["updatedBy"],
            ["updateTime"] = oldBooking["updateTime"],
            ["createdBy"] = oldBooking["createdBy"],
            ["createTime"] = oldBooking["createTime"],
            ["grossTotal"] = status == 2 ? 0 : serviceGrossTotal,
            ["netTotal"] = status == 2 ? 0 : serviceNetTotal
        };
    }

    private static (decimal bookingGrossTotal, decimal bookingNetTotal, decimal clientTotal) CalculateBookingTotals(JArray products, JObject oldBooking, decimal interest)
    {
        // 1. GrossTotal: Sum of all product gross totals
        decimal bookingGrossTotal = 0;
        foreach (var product in products)
        {
            var productGrossTotal = product["grossTotal"] != null ? product["grossTotal"].Value<decimal>() : 0;
            bookingGrossTotal += productGrossTotal;
        }
        bookingGrossTotal = Math.Round(bookingGrossTotal, 2);

        // 2. NetTotal: Sum of all product net totals
        decimal bookingNetTotal = 0;
        foreach (var product in products)
        {
            var productNetTotal = product["netTotal"] != null ? product["netTotal"].Value<decimal>() : 0;
            bookingNetTotal += productNetTotal;
        }
        bookingNetTotal = Math.Round(bookingNetTotal, 2);

        // 3. ClientTotal: Complex calculation with ClientPrice logic
        var clientPrice = oldBooking["clientPrice"] as JObject;
        var extras = clientPrice?["extras"] as JArray ?? new JArray();
        var subsidies = clientPrice?["subsidies"] as JArray ?? new JArray();
        
        decimal clientTotal = 0;
        // Sum extras
        foreach (var extra in extras)
        {
            clientTotal += extra["extra"] != null ? extra["extra"].Value<decimal>() : 0;
        }
        // Subtract subsidies
        foreach (var subsidy in subsidies)
        {
            clientTotal -= subsidy["extra"] != null ? subsidy["extra"].Value<decimal>() : 0;
        }
        // Add interest
        clientTotal += interest;
        clientTotal = Math.Round(clientTotal, 2);
        
        // Apply bounds checking (following Booking.ClientTotal logic)
        if (bookingGrossTotal > 0 && clientTotal > bookingGrossTotal) 
            clientTotal = bookingGrossTotal;
        if (clientTotal < 0) 
            clientTotal = 0;

        return (bookingGrossTotal, bookingNetTotal, clientTotal);
    }

    private static JObject CreateNewBookingObject(JObject oldBooking, JArray products, decimal bookingGrossTotal, decimal bookingNetTotal, decimal clientTotal)
    {
        return new JObject
        {
            // Basic fields
            ["id"] = oldBooking["id"],
            ["status"] = oldBooking["status"],
            ["clientPrice"] = oldBooking["clientPrice"],
            ["updatedBy"] = oldBooking["updatedBy"],
            ["updateTime"] = oldBooking["updateTime"],
            ["createdBy"] = oldBooking["createdBy"],
            ["createTime"] = oldBooking["createTime"],
            ["notes"] = oldBooking["notes"] ?? new JArray(),
            ["history"] = oldBooking["history"] ?? new JArray(),
            ["subsidyComment"] = oldBooking["subsidyComment"],
            ["salary"] = oldBooking["salary"],
            ["isGroupBooking"] = oldBooking["isGroupBooking"],
            ["products"] = products,
            ["clientTotal"] = clientTotal,
            ["grossTotal"] = bookingGrossTotal,
            ["netTotal"] = bookingNetTotal,
            ["segmentId"] = oldBooking["segmentId"],
            ["subSegmentId"] = oldBooking["subSegmentId"],
            ["externalOrderId"] = oldBooking["externalOrderId"],
            ["ownerKey"] = oldBooking["ownerKey"],
            ["period"] = oldBooking["period"],
            ["category"] = oldBooking["category"],
            ["tmura"] = oldBooking["tmura"],
            ["ccPayments"] = oldBooking["ccPayments"],
            ["salaryPayments"] = oldBooking["salaryPayments"],
            ["specialRequests"] = oldBooking["specialRequests"] ?? new JArray(),
            ["bookingTags"] = oldBooking["bookingTags"],
            ["periodEntitledDays"] = oldBooking["periodEntitledDays"],
        };
    }

    private static bool ValidateAndDetermineMigration(JObject oldBooking, JObject newBooking)
    {
        // Validate that calculated totals match original totals
        var originalGrossTotal = oldBooking["grossTotal"] != null ? oldBooking["grossTotal"].Value<decimal>() : 0;
        var originalNetTotal = oldBooking["netTotal"] != null ? oldBooking["netTotal"].Value<decimal>() : 0;
        var originalClientTotal = oldBooking["clientTotal"] != null ? oldBooking["clientTotal"].Value<decimal>() : 0;
        
        var calculatedGrossTotal = newBooking["grossTotal"] != null ? newBooking["grossTotal"].Value<decimal>() : 0;
        var calculatedNetTotal = newBooking["netTotal"] != null ? newBooking["netTotal"].Value<decimal>() : 0;
        var calculatedClientTotal = newBooking["clientTotal"] != null ? newBooking["clientTotal"].Value<decimal>() : 0;
        
        // Check if totals match (with small tolerance for rounding differences)
        const decimal tolerance = 0.01m;
        bool grossMatches = Math.Abs(originalGrossTotal - calculatedGrossTotal) <= tolerance;
        bool netMatches = Math.Abs(originalNetTotal - calculatedNetTotal) <= tolerance;
        bool clientMatches = Math.Abs(originalClientTotal - calculatedClientTotal) <= tolerance;
        
        bool totalsMatch = grossMatches && netMatches && clientMatches;
        
        // Check for missing fields in the new booking
        bool shouldSkipMigrationDueToMissingFields = ShouldSkipMigrationDueToMissingFields(oldBooking, newBooking);
        
        // Should insert only if totals match AND no missing fields
        bool shouldInsert = totalsMatch && !shouldSkipMigrationDueToMissingFields;
        
        if (!shouldInsert)
        {
            var skipReasons = new List<string>();
            
            if (!totalsMatch)
            {
                var totalsMismatchReason = $"Totals mismatch - Original: Gross={originalGrossTotal}, Net={originalNetTotal}, Client={originalClientTotal} | Calculated: Gross={calculatedGrossTotal}, Net={calculatedNetTotal}, Client={calculatedClientTotal}";
                skipReasons.Add(totalsMismatchReason);
                Console.WriteLine($"⚠️  Totals mismatch for booking ID: {oldBooking["id"]}");
                Console.WriteLine($"   Original - Gross: {originalGrossTotal}, Net: {originalNetTotal}, Client: {originalClientTotal}");
                Console.WriteLine($"   Calculated - Gross: {calculatedGrossTotal}, Net: {calculatedNetTotal}, Client: {calculatedClientTotal}");
                Console.WriteLine($"   Skipping migration due to totals mismatch");
            }
            if (shouldSkipMigrationDueToMissingFields)
            {
                skipReasons.Add("Missing fields in new booking structure");
                Console.WriteLine($"⚠️  Skipping migration due to missing fields for booking ID: {oldBooking["id"]}");
            }
            
            // Log skipped booking to file
            LogSkippedBooking(oldBooking, skipReasons);
        }
        
        return shouldInsert;
    }

    private static bool ShouldSkipMigrationDueToMissingFields(JObject oldBooking, JObject newBooking)
    {
        // Define fields that are known to be mapped to new structure
        var knownMappedFields = new HashSet<string>
        {
            // Basic fields that are directly mapped
            "id", "status", "updatedBy", "updateTime", "createdBy", "createTime",
            "notes", "history", "subsidyComment", "salary", "isGroupBooking",
            "clientTotal", "grossTotal", "netTotal", "segmentId", "subSegmentId",
            "externalOrderId", "ownerKey", "period", "category", "tmura",
            "ccPayments", "salaryPayments", "specialRequests", "bookingTags",
            "periodEntitledDays", "clientPrice", "products",
            
            // Fields that are mapped to products
            "atlantisHotelId", "apollo", "hotelBookingId", "hotelSegmentId",
            "interest", "netAdjustment", "cancellationFeeNet", "cancellationFeeGross",
            "cancelledTime", "cancellationReason",
            
            // Fields that are mapped to rooms (handled in products)
            "rooms",
            "board",
            "clientComment",
            "hotelComment",

            // Fields that are mapped to services (handled in products)
            "services"
        };

        // Define fields that are intentionally ignored (not mapped to new booking)
        var ignoredFields = new HashSet<string>
        {
            "pelecardTransactionId",
            "clientDue",
            "fullPayment",
            "subsidyDue"
        };

        // Check for missing primitive fields in old booking
        var missingFields = new List<(string fieldName, string value)>();
        var ignoredFieldsData = new List<(string fieldName, string value)>();
        
        foreach (var property in oldBooking.Properties())
        {
            var fieldName = property.Name;
            
            // Skip if this field is known to be mapped
            if (knownMappedFields.Contains(fieldName))
                continue;
                
            // Skip if it's an object or array (these are handled separately)
            if (property.Value.Type == JTokenType.Object || property.Value.Type == JTokenType.Array)
                continue;

            // Check if this is an intentionally ignored field
            if (ignoredFields.Contains(fieldName))
            {
                var value = oldBooking[fieldName]?.ToString() ?? "null";
                ignoredFieldsData.Add((fieldName, value));
                continue;
            }
                
            // Check if this primitive field exists in new booking
            if (newBooking[fieldName] == null)
            {
                var value = oldBooking[fieldName]?.ToString() ?? "null";
                missingFields.Add((fieldName, value));
            }
        }

        // Store ignored fields to file (always log these, but don't skip migration)
        if (ignoredFieldsData.Count > 0)
        {
            var bookingId = oldBooking["id"]?.ToString() ?? "unknown";
            StoreIgnoredFieldsToFile(bookingId, ignoredFieldsData);
            
            Console.WriteLine($"📝 Ignored fields for booking ID: {bookingId}");
            foreach (var (fieldName, value) in ignoredFieldsData)
            {
                Console.WriteLine($"   Ignored: {fieldName} = {value}");
            }
        }

        // Store missing fields to file if any found
        if (missingFields.Count > 0)
        {
            var bookingId = oldBooking["id"]?.ToString() ?? "unknown";
            var stored = StoreMissingFieldsToFile(bookingId, missingFields);
            
            Console.WriteLine($"🔍 Missing fields in new booking for ID: {bookingId}");
            foreach (var (fieldName, value) in missingFields)
            {
                Console.WriteLine($"   Missing: {fieldName} = {value}");
            }
            // Has missing fields and it was stored. Hence proceed with migration
            // If it was not stored, then skip migration
            return !stored;
        }
        
        return false; // No missing fields. Hence proceed with migration
    }


    private static bool StoreMissingFieldsToFile(string bookingId, List<(string fieldName, string value)> missingFields)
    {
        try
        {
            var projectDir = Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(AppDomain.CurrentDomain.BaseDirectory)));
            var fileName = Path.Combine(projectDir, "missing_fields_report.txt");
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            
            using (var writer = new StreamWriter(fileName, true))
            {
                writer.WriteLine($"=== Booking ID: {bookingId} - {timestamp} ===");
                foreach (var (fieldName, value) in missingFields)
                {
                    writer.WriteLine($"Missing Field: {fieldName} = {value}");
                }
                writer.WriteLine(); // Empty line for separation
            }
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Failed to write missing fields to file: {ex.Message}");
            return false;
        }
    }

    private static void LogSkippedBooking(JObject oldBooking, List<string> skipReasons)
    {
        try
        {
            var projectDir = Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(AppDomain.CurrentDomain.BaseDirectory)));
            var fileName = Path.Combine(projectDir, SKIPPED_BOOKINGS_FILE);
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            var bookingId = oldBooking["id"]?.ToString() ?? "unknown";
            
            using (var writer = new StreamWriter(fileName, true))
            {
                writer.WriteLine($"=== SKIPPED BOOKING ID: {bookingId} - {timestamp} ===");
                writer.WriteLine($"Status: {oldBooking["status"]?.ToString() ?? "null"}");
                writer.WriteLine($"Client Total: {oldBooking["clientTotal"]?.ToString() ?? "null"}");
                writer.WriteLine($"Gross Total: {oldBooking["grossTotal"]?.ToString() ?? "null"}");
                writer.WriteLine($"Net Total: {oldBooking["netTotal"]?.ToString() ?? "null"}");
                writer.WriteLine($"Skip Reasons:");
                foreach (var reason in skipReasons)
                {
                    writer.WriteLine($"  - {reason}");
                }
                writer.WriteLine($"Raw Booking Data: {oldBooking.ToString(Newtonsoft.Json.Formatting.Indented)}");
                writer.WriteLine(); // Empty line for separation
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Failed to write skipped booking to file: {ex.Message}");
        }
    }

    private static void StoreIgnoredFieldsToFile(string bookingId, List<(string fieldName, string value)> ignoredFields)
    {
        try
        {
            var projectDir = Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(AppDomain.CurrentDomain.BaseDirectory)));
            var fileName = Path.Combine(projectDir, IGNORED_FIELDS_FILE);
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            
            using (var writer = new StreamWriter(fileName, true))
            {
                writer.WriteLine($"=== IGNORED FIELDS - Booking ID: {bookingId} - {timestamp} ===");
                foreach (var (fieldName, value) in ignoredFields)
                {
                    writer.WriteLine($"Ignored Field: {fieldName} = {value}");
                }
                writer.WriteLine(); // Empty line for separation
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Failed to write ignored fields to file: {ex.Message}");
        }
    }

    private static void LogErrorBooking(JObject oldBooking, Exception ex)
    {
        try
        {
            var projectDir = Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(AppDomain.CurrentDomain.BaseDirectory)));
            var fileName = Path.Combine(projectDir, ERROR_BOOKINGS_FILE);
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            var bookingId = oldBooking["id"]?.ToString() ?? "unknown";
            
            using (var writer = new StreamWriter(fileName, true))
            {
                writer.WriteLine($"=== ERROR BOOKING ID: {bookingId} - {timestamp} ===");
                writer.WriteLine($"Error Message: {ex.Message}");
                writer.WriteLine($"Error Type: {ex.GetType().Name}");
                writer.WriteLine($"Stack Trace: {ex.StackTrace}");
                writer.WriteLine($"Status: {oldBooking["status"]?.ToString() ?? "null"}");
                writer.WriteLine($"Client Total: {oldBooking["clientTotal"]?.ToString() ?? "null"}");
                writer.WriteLine($"Gross Total: {oldBooking["grossTotal"]?.ToString() ?? "null"}");
                writer.WriteLine($"Net Total: {oldBooking["netTotal"]?.ToString() ?? "null"}");
                writer.WriteLine($"Raw Booking Data: {oldBooking.ToString(Newtonsoft.Json.Formatting.Indented)}");
                writer.WriteLine(); // Empty line for separation
            }
        }
        catch (Exception logEx)
        {
            Console.WriteLine($"⚠️  Failed to write error booking to file: {logEx.Message}");
        }
    }

    // Progress tracking classes and methods
    public class MigrationProgress
    {
        public int LastProcessedOffset { get; set; } = -1;
        public int TotalSuccessCount { get; set; } = 0;
        public int TotalErrorCount { get; set; } = 0;
        public int TotalSkippedCount { get; set; } = 0;
        public DateTime LastUpdated { get; set; } = DateTime.Now;
    }

    private static MigrationProgress? LoadProgress()
    {
        try
        {
            if (File.Exists(PROGRESS_FILE))
            {
                var json = File.ReadAllText(PROGRESS_FILE);
                return Newtonsoft.Json.JsonConvert.DeserializeObject<MigrationProgress>(json);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Could not load progress file: {ex.Message}");
        }
        return null;
    }

    private static void SaveProgress(MigrationProgress progress)
    {
        try
        {
            var json = Newtonsoft.Json.JsonConvert.SerializeObject(progress, Newtonsoft.Json.Formatting.Indented);
            File.WriteAllText(PROGRESS_FILE, json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Could not save progress file: {ex.Message}");
        }
    }

    private static void ClearProgress()
    {
        try
        {
            if (File.Exists(PROGRESS_FILE))
            {
                File.Delete(PROGRESS_FILE);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Could not clear progress file: {ex.Message}");
        }
    }
}
