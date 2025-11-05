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
            var config = CreateMigrationConfig(TARGET_COLLECTION, TARGET_COLLECTION);
            
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
