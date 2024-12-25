using System.Data;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.SqlClient;

namespace ETL;

class Program
{
    static void Main(string[] args)
    {
        const string csvPath = "../../../../data-csv/sample-cab-data.csv";
        const string duplicatesPath = "../../../../data-csv/duplicates.csv";
        const string connectionString = "Server=NIKITA\\SQLEXPRESS;Database=TaxiData;Trusted_Connection=True;TrustServerCertificate=True;";
        /*
        const string connectionString = @"Server=(localdb)\MSSQLLocalDB;Database=TaxiData;Trusted_Connection=True;";
        */

        var processedRecords = new List<TaxiRide>();
        var duplicateRecords = new List<TaxiRide>();
        var seenRecords = new HashSet<string>(); // To track duplicates

        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            TrimOptions = TrimOptions.Trim
        };

        using (var reader = new StreamReader(csvPath))
        using (var csv = new CsvReader(reader, config))
        {
            csv.Context.RegisterClassMap<TaxiRideMap>();
            var records = csv.GetRecords<TaxiRide>();

            foreach (var record in records)
            {
                string key = $"{record.PickupDatetime}-{record.DropoffDatetime}-{record.PassengerCount}";

                if (seenRecords.Contains(key))
                {
                    duplicateRecords.Add(record);
                }
                else
                {
                    seenRecords.Add(key);
                    record.StoreAndFwdFlag = record.StoreAndFwdFlag == "Y" ? "Yes" : "No";
                    record.PickupDatetime = record.DropoffDatetime.ToUniversalTime();
                    record.DropoffDatetime = record.DropoffDatetime.ToUniversalTime();
                    processedRecords.Add(record);
                }
            }
        }

        // Export duplicates to CSV
        using (var writer = new StreamWriter(duplicatesPath))
        using (var csv = new CsvWriter(writer, config))
        {
            csv.WriteRecords(duplicateRecords);
        }

        // Bulk insert processed records
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = "TaxiRides";

                var dataTable = ToDataTable(processedRecords);
                bulkCopy.WriteToServer(dataTable);
            }
        }
    }

    static DataTable ToDataTable(IEnumerable<TaxiRide> records)
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("PickupDatetime", typeof(DateTime));
        table.Columns.Add("DropoffDatetime", typeof(DateTime));
        table.Columns.Add("PassengerCount", typeof(int));
        table.Columns.Add("TripDistance", typeof(double));
        table.Columns.Add("StoreAndFwdFlag", typeof(string));
        table.Columns.Add("PULocationID", typeof(int));
        table.Columns.Add("DOLocationID", typeof(int));
        table.Columns.Add("FareAmount", typeof(decimal));
        table.Columns.Add("TipAmount", typeof(decimal));

        foreach (var record in records)
        {
            
            table.Rows.Add(
                record.Id,
                record.PickupDatetime,
                record.DropoffDatetime,
                record.PassengerCount,
                record.TripDistance,
                record.StoreAndFwdFlag,
                record.PULocationID,
                record.DOLocationID,
                record.FareAmount,
                record.TipAmount
            );
        }

        return table;
    }
}

public class TaxiRide
{
    public int Id { get; set; }
    public DateTime PickupDatetime { get; set; }
    public DateTime DropoffDatetime { get; set; }
    public int? PassengerCount { get; set; }
    public double TripDistance { get; set; }
    public string? StoreAndFwdFlag { get; set; }
    public int? PULocationID { get; set; }
    public int? DOLocationID { get; set; }
    public decimal FareAmount { get; set; }
    public decimal TipAmount { get; set; }
}

public sealed class TaxiRideMap : ClassMap<TaxiRide>
{
    public TaxiRideMap()
    {
        Map(m => m.PickupDatetime).Name("tpep_pickup_datetime");
        Map(m => m.DropoffDatetime).Name("tpep_dropoff_datetime");
        Map(m => m.PassengerCount).Name("passenger_count");
        Map(m => m.TripDistance).Name("trip_distance");
        Map(m => m.StoreAndFwdFlag).Name("store_and_fwd_flag");
        Map(m => m.PULocationID).Name("PULocationID");
        Map(m => m.DOLocationID).Name("DOLocationID");
        Map(m => m.FareAmount).Name("fare_amount");
        Map(m => m.TipAmount).Name("tip_amount");
    }
}