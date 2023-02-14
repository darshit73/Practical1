

using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

class Program
{
    private static string connectionString =
        "Data Source=DESKTOP-1VJLI7P;Initial Catalog=practical1db;Integrated Security=True";
    private  static string createTableQuery = "CREATE TABLE SourceTable(ID INT IDENTITY(1,1) PRIMARY KEY, FirstNumber INT, SecondNumber INT)";
    static readonly SqlConnection connection = new SqlConnection(connectionString);

    static void Main(string[] args)
    {
      
        CreateSourceTable();
        CreateDestinationTable();
    }

    private static void CreateDestinationTable()
    {   
             List<MigrationRecord> migrationRecords = new List<MigrationRecord>();
             for (int i = 0; i<=100000; i++)
             {
                 MigrationRecord record = ReadMigrationRecord(i);
                 if (record != null)
                 {
                     migrationRecords.Add(record);
                 }
             }
             
             if (migrationRecords.Count > 0)
             {
                 using (SqlConnection connection = new SqlConnection(connectionString))
                 {
                     connection.Open();

                     SqlCommand command = new SqlCommand("INSERT INTO DestinationTable (ID, Sum) VALUES (@ID, @Sum)", connection);
                     command.Parameters.AddWithValue("@ID", 0);
                     command.Parameters.AddWithValue("@Sum", 0);

                     foreach (MigrationRecord record in migrationRecords)
                     {
                         command.Parameters["@ID"].Value = record.Id;
                         command.Parameters["@Sum"].Value = record.Sum;

                         command.ExecuteNonQuery();
                         connection.Close();
                     }
                 }
             }
    }

    private static void CreateSourceTable()
    {
        connection.Open();
        SqlCommand sqlCommand = new SqlCommand(createTableQuery, connection);
        sqlCommand.ExecuteNonQuery();
        connection.Close();
        
        DataTable tbl = new DataTable();
        tbl.Columns.Add(new DataColumn("FirstNumber", typeof(Int32)));  
        tbl.Columns.Add(new DataColumn("SecondNumber", typeof(Int32)));  
      
        for(int i=0; i<10000; i++)  
        {   
            DataRow dr = tbl.NewRow();
            dr["FirstNumber"] = i;
            dr["SecondNumber"] = i * 2;
            tbl.Rows.Add(dr);   
        }  
        
        SqlBulkCopy objbulk = new SqlBulkCopy(connection);
        objbulk.DestinationTableName = "SourceTable";  
        
        
        objbulk.ColumnMappings.Add("FirstNumber", "FirstNumber");   
        objbulk.ColumnMappings.Add("SecondNumber", "SecondNumber");
        
        Stopwatch timer = new Stopwatch();
        timer.Start();
        
        connection.Open();
        objbulk.WriteToServer(tbl);  
        connection.Close();  
        
        timer.Stop();
        Console.WriteLine($"Time elapsed: {timer.ElapsedMilliseconds}");

       
        // time taken to add data using for loop for 1M data it take more than 1 minute
        //using above bulkCopy methode take 0.8 sec .
           
            // Loop to insert one million records into the database
            // for (int i = 1; i <= 1000000; i++)
            // {
            //     // Create a SQL command object
            //     using SqlCommand command = new SqlCommand(
            //         "INSERT INTO SourceTable(FirstNumber, SecondNumber) VALUES (@param1, @param2)", connection);
            //     // Add the parameters to the command object
            //     command.Parameters.AddWithValue("@param1", i);
            //     command.Parameters.AddWithValue("@param2", i * 2);
            //     // Execute the SQL command
            //     command.ExecuteNonQuery();
            // }
            // connection.Close();
        
     
    }
    static MigrationRecord ReadMigrationRecord(int id)
    {
        MigrationRecord record = null;
        connection.Open();

            SqlCommand command = new SqlCommand($"SELECT * FROM SourceTable WHERE ID = {id}", connection);

            using (SqlDataReader reader = command.ExecuteReader())
            {
                if (reader.Read())
                {
                    int firstNumber = (int)reader["FirstNumber"];
                    int secondNumber = (int)reader["SecondNumber"];

                    int sum = firstNumber + secondNumber;
                    record = new MigrationRecord(id, sum);
                }
            }

            return record;
    }
    
}
class MigrationRecord
{
    public int Id { get; set; }
    public int Sum { get; set; }

    public MigrationRecord(int id, int sum)
    {
        Id = id;
        Sum = sum;
    }
}


