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
    private const int BATCH_SIZE = 200; // Process bookings in batches of 200
    // Log file names (without paths - paths will be generated dynamically)
    private const string SKIPPED_BOOKINGS_FILE_NAME = "skipped_bookings_report.txt";
    private const string ERROR_BOOKINGS_FILE_NAME = "error_bookings_report.txt";
    private const string MISSING_FIELDS_FILE_NAME = "missing_fields_report.txt";
    private const string TOTALS_MISMATCH_FILE_NAME = "totals_mismatch_report.txt";
    private const bool FORCE_BOOKING_CREATION = true;
    
    // Static variables for current run
    private static string? _currentRunLogsDirectory;
    private static string? _currentRunId;

    static async Task Main(string[] args)
    {
        try
        {
            Console.WriteLine("Starting booking migration process...");
            
            // Initialize logging directory for this run
            InitializeLoggingDirectory();
            
            // Initialize Couchbase connection
            var cluster = await InitializeCouchbaseConnection();
            var bucket = await GetBucket(cluster);
            
            // Get all booking IDs that need migration (separated by insert/update)
            var (insertBookings, updateBookings) = await GetBookingIdsToMigrate(cluster);
            var totalBookings = insertBookings.Count + updateBookings.Count;
            Console.WriteLine($"Total bookings to process: {totalBookings} (INSERT: {insertBookings.Count}, UPDATE: {updateBookings.Count})");
            
            if (totalBookings == 0)
            {
                Console.WriteLine("No bookings found to migrate.");
                return;
            }
            
            // Process insert bookings
            var insertResults = await ProcessBookingsByIds(cluster, bucket, insertBookings, "INSERT");
            Console.WriteLine($"\nInsert Migration completed!");
            Console.WriteLine($"✅ Success: {insertResults.successCount}");
            Console.WriteLine($"❌ Errors: {insertResults.errorCount}");
            Console.WriteLine($"⚠️  Skipped (totals mismatch): {insertResults.skippedCount}");
            
            // Process update bookings
            var updateResults = await ProcessBookingsByIds(cluster, bucket, updateBookings, "UPDATE");
            
            // Display final results
            Console.WriteLine($"\nUpdate Migration completed!");
            Console.WriteLine($"✅ Success: {updateResults.successCount}");
            Console.WriteLine($"❌ Errors: {updateResults.errorCount}");
            Console.WriteLine($"⚠️  Skipped (totals mismatch): {updateResults.skippedCount}");
            
            // Update ID counter to prevent ID conflicts for new bookings
            await UpdateIdCounter(cluster, bucket);
            
            // Cleanup
            await cluster.DisposeAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Migration failed: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
        }
    }
    private static void InitializeLoggingDirectory()
    {
        try
        {
            // Create run ID with timestamp
            _currentRunId = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
            
            // Get project directory
            var projectDir = Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(AppDomain.CurrentDomain.BaseDirectory)));
            
            // Create logs directory structure
            var logsBaseDir = Path.Combine(projectDir, "logs");
            _currentRunLogsDirectory = Path.Combine(logsBaseDir, $"migration_run_{_currentRunId}");
            
            // Create the directory if it doesn't exist
            Directory.CreateDirectory(_currentRunLogsDirectory);
            
            Console.WriteLine($"📁 Logs directory created: {_currentRunLogsDirectory}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Failed to create logs directory: {ex.Message}");
            // Fallback to current directory
            _currentRunLogsDirectory = Directory.GetCurrentDirectory();
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
    private static async Task<Dictionary<string, JObject>> FetchBookingsBatch(ICluster cluster, List<string> bookingIds)
    {
        var bookings = new Dictionary<string, JObject>();
        
        if (bookingIds.Count == 0)
        {
            return bookings;
        }

        Console.WriteLine($"Fetching batch of {bookingIds.Count} bookings...");
        
        // Use Couchbase array parameter for proper batch query
        // Convert string booking IDs to uint for comparison
        var numericBookingIds = bookingIds.Select(id => uint.TryParse(id, out var numId) ? numId : (uint?)null)
                                         .Where(id => id.HasValue)
                                         .Select(id => id.Value)
                                         .ToList();
        
        var query = $"SELECT {OLD_COLLECTION}.* FROM `{BUCKET_NAME}`.`{SCOPE}`.`{OLD_COLLECTION}` WHERE id IN $bookingIds AND atlantisHotelId IS NOT NULL";
        
        var result = await cluster.QueryAsync<JObject>(query, options => options.Parameter("bookingIds", numericBookingIds));
        
        await foreach (var row in result)
        {
            if (row != null && row["id"] != null && row["atlantisHotelId"] != null)
            {
                var bookingId = row["id"].ToString();
                bookings[bookingId] = row;
            }
        }
        
        Console.WriteLine($"Successfully fetched {bookings.Count} bookings from batch");
        return bookings;
    }

    private static async Task<(List<string> insertBookings, List<string> updateBookings)> GetBookingIdsToMigrate(ICluster cluster)
    {
        Console.WriteLine("Getting booking IDs that need migration...");
        // Get bookings with null values that need updating
        var bookingsWithNullValues = await GetBookingsWithNullValues(cluster);
        Console.WriteLine($"Found {bookingsWithNullValues.Count} bookings with null fields that need updating");
        
        // Get all booking IDs from new collection with their updateTime
        var newQuery = $"SELECT id, updateTime FROM `{BUCKET_NAME}`.`{SCOPE}`.`{NEW_COLLECTION}`";
        var migratedBookings = new Dictionary<string, DateTime>();
        var newResult = await cluster.QueryAsync<JObject>(newQuery);
        
        await foreach (var row in newResult)
        {
            if (row != null && row["id"] != null)
            {
                var bookingId = row["id"].ToString();
                DateTime updateTime = DateTime.MinValue;
                
                if (row["updateTime"] != null && row["updateTime"].Type != JTokenType.Null)
                {
                    if (DateTime.TryParse(row["updateTime"].ToString(), out var parsedTime))
                    {
                        updateTime = parsedTime;
                    }
                }
                
                migratedBookings[bookingId] = updateTime;
            }
        }
        
        Console.WriteLine($"Found {migratedBookings.Count} already migrated bookings");
        
        // Get all old bookings that need migration
        var oldQuery = $"SELECT id, updateTime FROM `{BUCKET_NAME}`.`{SCOPE}`.`{OLD_COLLECTION}` WHERE atlantisHotelId IS NOT NULL";
        var insertBookings = new List<string>();
        var updateBookings = new List<string>();
        
        var oldResult = await cluster.QueryAsync<JObject>(oldQuery);
        
        await foreach (var row in oldResult)
        {
            if (row["id"] != null)
            {
                var bookingId = row["id"].ToString();
                
                if (!migratedBookings.ContainsKey(bookingId))
                {
                    // New booking - needs insertion
                    insertBookings.Add(bookingId);
                }
                else
                {
                    // Existing booking - check if old version is newer
                    DateTime oldUpdateTime = DateTime.MinValue;
                    
                    if (row["updateTime"] != null && row["updateTime"].Type != JTokenType.Null)
                    {
                        if (DateTime.TryParse(row["updateTime"].ToString(), out var parsedTime))
                        {
                            oldUpdateTime = parsedTime;
                        }
                    }
                    
                    var newUpdateTime = migratedBookings[bookingId];
                    if (oldUpdateTime > newUpdateTime)
                    {
                        Console.WriteLine($"📝 Booking {bookingId} needs update (old: {oldUpdateTime:yyyy-MM-dd HH:mm:ss}, new: {newUpdateTime:yyyy-MM-dd HH:mm:ss})");
                        updateBookings.Add(bookingId);
                    }
                    else if (bookingsWithNullValues.Contains(bookingId))
                    {
                        Console.WriteLine($"📝 Booking {bookingId} needs update (null values)");
                        updateBookings.Add(bookingId);
                    }
                }
            }
        }
        
        Console.WriteLine($"Found {insertBookings.Count} bookings to INSERT and {updateBookings.Count} bookings to UPDATE");
        return (insertBookings, updateBookings);
    }

    private static async Task<List<string>> GetBookingsWithNullValues(ICluster cluster)
    {
        Console.WriteLine("Getting bookings with null periodEntitledDays or cancelledTime...");
        
        var nullFieldsQuery = $@"
            SELECT id FROM `{BUCKET_NAME}`.`{SCOPE}`.`{NEW_COLLECTION}`
            WHERE periodEntitledDays IS NULL
            OR ANY p IN products SATISFIES p.status = 2 AND p.cancellationDetails IS NOT NULL 
            AND (p.cancellationDetails.cancelledTime IS NULL) END";
        
        var bookingsWithNullValues = new List<string>();
        var nullFieldsResult = await cluster.QueryAsync<JObject>(nullFieldsQuery);
        
        await foreach (var row in nullFieldsResult)
        {
            if (row != null && row["id"] != null)
            {
                bookingsWithNullValues.Add(row["id"].ToString());
            }
        }
        
        return bookingsWithNullValues;
    }

    private static async Task<(int successCount, int errorCount, int skippedCount)> ProcessBookingsByIds(ICluster cluster, IBucket bucket, List<string> bookingIds, string operationType)
    {
        int successCount = 0;
        int errorCount = 0;
        int skippedCount = 0;

        if (bookingIds.Count == 0)
        {
            return (successCount, errorCount, skippedCount);
        }

        Console.WriteLine($"Processing {bookingIds.Count} bookings for {operationType} in batches of {BATCH_SIZE}...");

        // Process bookings in batches
        for (int batchStart = 0; batchStart < bookingIds.Count; batchStart += BATCH_SIZE)
        {
            var batchEnd = Math.Min(batchStart + BATCH_SIZE, bookingIds.Count);
            var batchIds = bookingIds.GetRange(batchStart, batchEnd - batchStart);
            
            Console.WriteLine($"\n=== Processing {operationType} batch {batchStart / BATCH_SIZE + 1} (bookings {batchStart + 1}-{batchEnd}) ===");
            
            // Fetch all bookings in this batch
            var batchBookings = await FetchBookingsBatch(cluster, batchIds);
            
            // Process each booking in the batch
            for (int i = 0; i < batchIds.Count; i++)
            {
                try
                {
                    var bookingId = batchIds[i];
                    var globalIndex = batchStart + i + 1;
                    Console.WriteLine($"\n--- Processing booking {globalIndex}/{bookingIds.Count}: {bookingId} ---");
                    
                    // Get booking from batch-fetched data
                    var booking = batchBookings.ContainsKey(bookingId) ? batchBookings[bookingId] : null;
                    
                    if (booking == null)
                    {
                        LogErrorBooking(bookingId, new Exception($"Booking {bookingId} not found or missing atlantisHotelId"));
                        Console.WriteLine($"Booking {bookingId} not found or missing atlantisHotelId");
                        errorCount++;
                        continue;
                    }

                    Console.WriteLine($"🔄 {operationType} operation for booking {bookingId}");

                    // Process single booking
                    var isUpdate = operationType == "UPDATE";
                    var result = await ProcessSingleBooking(bucket, booking, isUpdate);
                    
                    // Update totals
                    if (result == "success")
                    {
                        successCount++;
                    }
                    else if (result == "skipped")
                    {
                        skippedCount++;
                    }
                    else
                    {
                        errorCount++;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Error processing booking {batchIds[i]}: {ex.Message}");
                    errorCount++;
                }
            }
            
            Console.WriteLine($"=== Completed {operationType} batch {batchStart / BATCH_SIZE + 1} ===");
        }

        return (successCount, errorCount, skippedCount);
    }


    private static async Task<string> ProcessSingleBooking(IBucket bucket, JObject oldBooking, bool isUpdate = false)
    {
        var operation = isUpdate ? "updating" : "migrating";
        try
        {
            var bookingId = oldBooking["id"]?.ToString() ?? "unknown";
            Console.WriteLine($"Processing booking ID: {bookingId} ({operation})");
            
            // Convert to new booking format and validate totals
            var (newBooking, shouldInsert) = ConvertToNewBooking(oldBooking);
            
            if (!shouldInsert)
            {
                Console.WriteLine($"⚠️ Skipped booking ID: {bookingId}");
                return "skipped";
            }
            
            // Insert or update in new collection
            await InsertBookingToNewCollection(bucket, oldBooking, newBooking);
            
            Console.WriteLine($"✅ Successfully processed booking ID: {bookingId} {operation}");
            return "success";
        }
        catch (Exception ex)
        {
            var bookingId = oldBooking["id"]?.ToString() ?? "unknown";
            Console.WriteLine($"❌ Failed to process booking ID: {bookingId} {operation} - {ex.Message}");
            
            // Log error booking with full details
            try
            {
                LogErrorBooking(bookingId, ex);
                Console.WriteLine($"📝 Error logged for booking ID: {bookingId}");
            }
            catch (Exception logEx)
            {
                Console.WriteLine($"⚠️  Failed to log error for booking {bookingId}: {logEx.Message}");
            }
            
            return "error";
        }
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
                hotelGrossTotal += room["gross"] != null && room["gross"].Type != JTokenType.Null ? room["gross"].Value<decimal>() : 0;
                hotelNetTotal += room["net"] != null && room["net"].Type != JTokenType.Null ? room["net"].Value<decimal>() : 0;
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
                ["cancelledTime"] = oldBooking["cancelledTime"] ?? DateTime.Now,
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
            ["periodEntitledDays"] = oldBooking["periodEntitledDays"] ?? 0,
        };
    }

    private static bool ValidateAndDetermineMigration(JObject oldBooking, JObject newBooking)
    {
        // Validate that calculated totals match original totals
        var originalGrossTotal = oldBooking["grossTotal"] != null && oldBooking["grossTotal"].Type != JTokenType.Null ? oldBooking["grossTotal"].Value<decimal>() : 0;
        var originalNetTotal = oldBooking["netTotal"] != null && oldBooking["netTotal"].Type != JTokenType.Null ? oldBooking["netTotal"].Value<decimal>() : 0;
        var originalClientTotal = oldBooking["clientTotal"] != null && oldBooking["clientTotal"].Type != JTokenType.Null ? oldBooking["clientTotal"].Value<decimal>() : 0;
        
        var calculatedGrossTotal = newBooking["grossTotal"] != null && newBooking["grossTotal"].Type != JTokenType.Null ? newBooking["grossTotal"].Value<decimal>() : 0;
        var calculatedNetTotal = newBooking["netTotal"] != null && newBooking["netTotal"].Type != JTokenType.Null ? newBooking["netTotal"].Value<decimal>() : 0;
        var calculatedClientTotal = newBooking["clientTotal"] != null && newBooking["clientTotal"].Type != JTokenType.Null ? newBooking["clientTotal"].Value<decimal>() : 0;
        
        // Check if totals match (with small tolerance for rounding differences)
        const decimal tolerance = 0.01m;
        bool grossMatches = Math.Abs(originalGrossTotal - calculatedGrossTotal) <= tolerance;
        bool netMatches = Math.Abs(originalNetTotal - calculatedNetTotal) <= tolerance;
        bool clientMatches = Math.Abs(originalClientTotal - calculatedClientTotal) <= tolerance;
        
        bool totalsMatch = grossMatches && netMatches && clientMatches;
            
        if (!totalsMatch)
        {
            Console.WriteLine($"⚠️  Totals mismatch for booking ID: {oldBooking["id"]}");
            Console.WriteLine($"   Original - Gross: {originalGrossTotal}, Net: {originalNetTotal}, Client: {originalClientTotal}");
            Console.WriteLine($"   Calculated - Gross: {calculatedGrossTotal}, Net: {calculatedNetTotal}, Client: {calculatedClientTotal}");
            Console.WriteLine($"   Skipping migration due to totals mismatch: {FORCE_BOOKING_CREATION.ToString()}");
            
            // Log totals mismatch to separate file
            var message = "Totals mismatch- Original: Gross=" + originalGrossTotal + ", Net=" + originalNetTotal + ", Client=" + originalClientTotal + " | Calculated: Gross=" + calculatedGrossTotal + ", Net=" + calculatedNetTotal + ", Client=" + calculatedClientTotal;
            LogTotalsMismatch(oldBooking, message);
        }
        var shouldSkipDueToTotalsMismatch = !totalsMatch && !FORCE_BOOKING_CREATION;

        
        // Check for missing fields in the new booking
        bool shouldSkipMigrationDueToMissingFields = ShouldSkipMigrationDueToMissingFields(oldBooking, newBooking);
        
        // Should insert only if totals match AND no missing fields
        bool shouldSkip = shouldSkipDueToTotalsMismatch || shouldSkipMigrationDueToMissingFields;
        
        if (shouldSkip)
        {
            var skipReasons = new List<string>();
            if (shouldSkipDueToTotalsMismatch)
            {
                var skipReason = "Totals mismatch- Original: Gross=" + originalGrossTotal + ", Net=" + originalNetTotal + ", Client=" + originalClientTotal + " | Calculated: Gross=" + calculatedGrossTotal + ", Net=" + calculatedNetTotal + ", Client=" + calculatedClientTotal;
                skipReasons.Add(skipReason);
            }
            if (shouldSkipMigrationDueToMissingFields)
            {
                var skipReason = "shouldSkipMigrationDueToMissingFields: " + shouldSkipMigrationDueToMissingFields.ToString();
                skipReasons.Add(skipReason);
            }
            // Log skipped booking to file
            LogSkippedBooking(oldBooking, skipReasons);
        }
        
        return !shouldSkip;
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
            "rooms", "board", "clientComment", "hotelComment",
            // Fields that are mapped to services (handled in products)
            "services"
        };

        // Check for missing primitive fields in old booking
        var missingFields = new List<(string fieldName, string value)>();
        
        foreach (var property in oldBooking.Properties())
        {
            var fieldName = property.Name;
            
            // Skip if this field is known to be mapped
            if (knownMappedFields.Contains(fieldName))
                continue;
                
            // Skip if it's an object or array (these are handled separately)
            if (property.Value.Type == JTokenType.Object || property.Value.Type == JTokenType.Array)
                continue;
                
            // Check if this primitive field exists in new booking
            if (newBooking[fieldName] == null)
            {
                var value = oldBooking[fieldName]?.ToString() ?? "null";
                missingFields.Add((fieldName, value));
            }
        }

        // If no missing fields, proceed with migration
        if (missingFields.Count == 0)
            return false;

        // Store missing fields to file and determine if migration should proceed
        var bookingId = oldBooking["id"]?.ToString() ?? "unknown";
        var stored = StoreMissingFieldsToFile(bookingId, missingFields);
        
        Console.WriteLine($"🔍 Missing fields in new booking for ID: {bookingId}");
        foreach (var (fieldName, value) in missingFields)
        {
            Console.WriteLine($"   Missing: {fieldName} = {value}");
        }
        
        // If stored successfully, proceed with migration; if storage failed, skip migration
        return !stored;
    }


    private static bool StoreMissingFieldsToFile(string bookingId, List<(string fieldName, string value)> missingFields)
    {
        try
        {
            var fileName = GetLogFilePath(MISSING_FIELDS_FILE_NAME);
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
            var fileName = GetLogFilePath(SKIPPED_BOOKINGS_FILE_NAME);
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            var bookingId = oldBooking["id"]?.ToString() ?? "unknown";
            
            using (var writer = new StreamWriter(fileName, true))
            {
                writer.WriteLine($"=== SKIPPED BOOKING ID: {bookingId} - {timestamp} ===");
                writer.WriteLine($"Status: {oldBooking["status"]?.ToString() ?? "null"}");
                writer.WriteLine($"Skip Reasons:");
                foreach (var reason in skipReasons)
                {
                    writer.WriteLine($"  - {reason}");
                }
                writer.WriteLine(); // Empty line for separation
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Failed to write skipped booking to file: {ex.Message}");
        }
    }

    private static void LogTotalsMismatch(JObject oldBooking, string message)
    {
        try
        {
            var fileName = GetLogFilePath(TOTALS_MISMATCH_FILE_NAME);
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            var bookingId = oldBooking["id"]?.ToString() ?? "unknown";
            
            using (var writer = new StreamWriter(fileName, true))
            {
                writer.WriteLine($"=== TOTALS MISMATCH - Booking ID: {bookingId} - {timestamp} ===");
                writer.WriteLine($"Message: {message}");
                writer.WriteLine(); // Empty line for separation
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Failed to write totals mismatch to file: {ex.Message}");
        }
    }

    private static void LogErrorBooking(string bookingId, Exception ex)
    {
        try
        {

            var fileName = GetLogFilePath(ERROR_BOOKINGS_FILE_NAME);
            
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            
            Console.WriteLine($"🔍 Writing error log for booking ID: {bookingId}");
            
            using (var writer = new StreamWriter(fileName, true))
            {
                writer.WriteLine($"=== ERROR BOOKING ID: {bookingId} - {timestamp} ===");
                writer.WriteLine($"Error Message: {ex.Message}");
                writer.WriteLine($"Error Type: {ex.GetType().Name}");
                writer.WriteLine($"Stack Trace: {ex.StackTrace}");
                writer.WriteLine(); // Empty line for separation
            }
            
            Console.WriteLine($"✅ Error log written successfully for booking ID: {bookingId}");
        }
        catch (Exception logEx)
        {
            Console.WriteLine($"⚠️  Failed to write error booking to file: {logEx.Message}");
            Console.WriteLine($"⚠️  Stack trace: {logEx.StackTrace}");
        }
    }


    private static string GetLogFilePath(string fileName)
    {
        if (string.IsNullOrEmpty(_currentRunLogsDirectory))
        {
            throw new InvalidOperationException("Logging directory not initialized. Call InitializeLoggingDirectory() first.");
        }
        return Path.Combine(_currentRunLogsDirectory, fileName);
    }

    private static async Task UpdateIdCounter(ICluster cluster, IBucket bucket)
    {
        try
        {
            Console.WriteLine($"\n🔢 Updating ID counter for {NEW_COLLECTION}...");
            
            // Query to get the maximum booking ID from the new collection
            var maxIdQuery = $"SELECT MAX(id) as maxId FROM `{BUCKET_NAME}`.`{SCOPE}`.`{NEW_COLLECTION}`";
            var maxIdResult = await cluster.QueryAsync<JObject>(maxIdQuery);
            
            uint maxId = 0;
            await foreach (var row in maxIdResult)
            {
                if (row != null && row["maxId"] != null && row["maxId"].Type != JTokenType.Null)
                {
                    maxId = row["maxId"].Value<uint>();
                }
            }
            
            Console.WriteLine($"📊 Highest booking ID in collection: {maxId}");
            
            // Get current counter value from the new collection
            var scopeObj = await bucket.ScopeAsync(SCOPE);
            var collection = await scopeObj.CollectionAsync(NEW_COLLECTION);
            
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
}
