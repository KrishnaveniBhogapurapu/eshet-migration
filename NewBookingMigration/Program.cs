using Couchbase;
using Newtonsoft.Json.Linq;

namespace NewBookingMigration;

class Program
{
    private const string CONNECTION_STRING = "vm2.server-6snuprr7isbki.northeurope.cloudapp.azure.com";
    private const string USERNAME = "";
    private const string PASSWORD = "";
    private const string BUCKET_NAME = "Apollo";
    private const string SCOPE = "Eshet";
    private const string TARGET_COLLECTION = "Booking2";
    private const int BATCH_SIZE = 200;
    
    // Static variables for current run
    private static string? _currentRunLogsDirectory;

    static async Task Main(string[] args)
    {
        InitializeLoggingDirectory();
        await RunBookingUpdateMigrationAsync();
        // await RunGroupBillingBookingMigrationAsync();
        await UpdateIdCounterAsync(TARGET_COLLECTION);
        
    }
    private static void InitializeLoggingDirectory()
    {
        try
        {
            // Create run ID with timestamp
            var currentRunId = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
            
            // Get project directory
            var projectDir = Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(AppDomain.CurrentDomain.BaseDirectory)));
            
            // Create logs directory structure
            var logsBaseDir = Path.Combine(projectDir, "logs");
            _currentRunLogsDirectory = Path.Combine(logsBaseDir, $"migration_run_{currentRunId}");
            
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


    private static MigrationConfig CreateMigrationConfig(string sourceCollection, string targetCollection)
    {
        return new MigrationConfig
        {
            ConnectionString = CONNECTION_STRING,
            Username = USERNAME,
            Password = PASSWORD,
            BucketName = BUCKET_NAME,
            Scope = SCOPE,
            SourceCollection = sourceCollection,
            TargetCollection = targetCollection,
            BatchSize = BATCH_SIZE,
            LogsDirectory = _currentRunLogsDirectory ?? throw new InvalidOperationException("Logging directory not initialized")
        };
    }

    private static async Task RunGroupBillingBookingMigrationAsync()
    {
        try
        {
            // Create migration configuration
            var config = CreateMigrationConfig("GroupBillingBookings", TARGET_COLLECTION);
            
            // Create migration instance
            var migration = new GroupBillingBookingMigration(config);
            
            // Run the migration
            await migration.RunMigrationAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Group Billing Booking migration failed: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
        }
    }

    private static async Task RunBookingUpdateMigrationAsync()
    {
        try
        {
            // Create migration configuration (source and target are the same collection)
            var config = CreateMigrationConfig("Bookings1", TARGET_COLLECTION);
            
            // Create migration instance
            var migration = new BookingUpdateMigration(config);
            
            // Run the migration
            await migration.RunMigrationAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Booking update migration failed: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
        }
    }

    private static async Task UpdateIdCounterAsync(string collectionName)
    {
        try
        {
            Console.WriteLine($"\n🔢 Updating ID counter for {collectionName}...");
            
            // Initialize Couchbase connection
            var cluster = await Cluster.ConnectAsync(CONNECTION_STRING, options =>
            {
                options.UserName = USERNAME;
                options.Password = PASSWORD;
                options.EnableTls = true;
                options.EnableDnsSrvResolution = true;
                options.KvCertificateCallbackValidation = (sender, certificate, chain, sslPolicyErrors) => true;
                options.HttpCertificateCallbackValidation = (sender, certificate, chain, sslPolicyErrors) => true;
            });
            
            var bucket = await cluster.BucketAsync(BUCKET_NAME);
            await bucket.WaitUntilReadyAsync(TimeSpan.FromSeconds(10));
            
            // Query to get the maximum booking ID from the target collection
            var maxIdQuery = $"SELECT MAX(id) as maxId FROM `{BUCKET_NAME}`.`{SCOPE}`.`{collectionName}`";
            var maxIdResult = await cluster.QueryAsync<JObject>(maxIdQuery);
            
            uint maxId = 0;
            await foreach (var row in maxIdResult)
            {
                if (row != null)
                {
                    var maxIdToken = row["maxId"];
                    if (maxIdToken != null && maxIdToken.Type != JTokenType.Null)
                    {
                        maxId = maxIdToken.Value<uint>();
                    }
                }
            }
            
            Console.WriteLine($"📊 Highest booking ID in collection: {maxId}");
            
            // Get current counter value from the target collection
            var scopeObj = await bucket.ScopeAsync(SCOPE);
            var collection = await scopeObj.CollectionAsync(collectionName);
            
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
            
            // Cleanup
            await cluster.DisposeAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"⚠️  Failed to update ID counter: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
        }
    }
}

internal class MigrationConfig
{
    public string ConnectionString { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public string BucketName { get; set; } = string.Empty;
    public string Scope { get; set; } = string.Empty;
    public string SourceCollection { get; set; } = string.Empty;
    public string TargetCollection { get; set; } = string.Empty;
    public string ClientInvoiceCollection { get; set; } = "ClientInvoices";
    public string VendorInvoiceCollection { get; set; } = "VendorInvoices";
    public string ReceiptCollection { get; set; } = "Receipts";
    public string PaymentCollection { get; set; } = "Payments";
    public int BatchSize { get; set; } = 200;
    public string LogsDirectory { get; set; } = string.Empty;
}
