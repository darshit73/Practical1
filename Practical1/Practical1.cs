using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace Practical1;

static class Practical1
{
    private static readonly string connectionString = "Data Source=DESKTOP-1VJLI7P;Initial Catalog=practical1db;Integrated Security=True";
    private static readonly string checkSourceTable = "SELECT CASE WHEN OBJECT_ID('dbo.SourceTable') IS NOT NULL THEN 1 ELSE 0 END";
    private static readonly string checkDestinationTable = "SELECT CASE WHEN OBJECT_ID('dbo.DestinationTable') IS NOT NULL THEN 1 ELSE 0 END";
    private  static readonly string createSourceTableQuery = "CREATE TABLE SourceTable(ID INT IDENTITY(1,1) PRIMARY KEY, FirstNumber INT, SecondNumber INT)";
    private static readonly string createDestinationTableQuery = "CREATE TABLE DestinationTable(ID INT IDENTITY(1,1) PRIMARY KEY,Fkey INT, Sum INT, FOREIGN KEY (Fkey) REFERENCES SourceTable(ID))";
    private static readonly SqlConnection Connection = new (connectionString);
    private static readonly int totalData = 1000000;
    private static readonly int batchSize = 100;
    private static int _currentIndex;
    private static int _completed;
    private static int _endNumber;
    private static Thread _thread = null!;
    private static int _startNumber;
    private static bool _createSourceTable;
    private static bool _createDestinationTable;

    static void Main()
    {
         _createSourceTable =CheckSourceTableExists();
         _createDestinationTable = CheckDestinationTableExists();
        start:
        Console.WriteLine("1. Create SourceTable\n" +
                          "2. Migrate Data from Source To Destination Table\n" +
                          "3. Status of your Migration\n" +
                          "4. Cancel Migration\n" +
                          "5. Drop Both Table\n"+
                          "6. Exit");
        
        Console.WriteLine("Enter one choice");
        int switchNum = Convert.ToInt32(Console.ReadLine());
        
        switch (switchNum)
        {
            case 1 :
                if (!_createSourceTable)
                {
                    _createSourceTable = true;
                    CreateSourceTable();
                    Console.WriteLine("Source Table Created Successfully ");
                }
                else
                {
                    Console.WriteLine("Table is Already Created");
                }
                break;
            case 2:
                if (_createSourceTable)
                {
                    _createDestinationTable = true;
                    
                    Console.WriteLine($"Total Number of record in source table is {totalData} how many you want to Migrate");
                    Console.WriteLine("Enter a startNumber : ");
                    _startNumber = Convert.ToInt32(Console.ReadLine());

                    Console.WriteLine("Enter a endNumber : ");
                    _endNumber = Convert.ToInt32(Console.ReadLine());
                    
                    //It will create new thread to create destination table.
                    _thread = new Thread(() => CreateDestinationTable(_startNumber,_endNumber));
                    _thread.Start();
                }
                else
                {
                    Console.WriteLine("Please create Source Table First");
                }
                break;
            case 3 :
                if (_createSourceTable && _createDestinationTable)
                {
                    Console.WriteLine($"Total number of data to insert is {_endNumber - _startNumber+1}\n " +
                                      $"Data added in Destination is {_completed}\n" +
                                      $"Data that left is {_endNumber - _startNumber - _completed+1}\n");
                }
                else
                {
                    Console.WriteLine("first create tables");
                }
                break;
            case 4:
                if (_createSourceTable && _createDestinationTable)
                {
                    _thread.Interrupt();
                    Console.WriteLine("Migration Canceled");
                }
                else
                {
                    Console.WriteLine("first create tables");
                }
               
                break;
            case 5 :
                if (_createSourceTable && _createDestinationTable)
                {
                    _createSourceTable = false;
                    _createDestinationTable = false;
                    DropTables();
                    Console.WriteLine("Table Drop Successfully");
                }
                else
                {
                    Console.WriteLine("Table Dose Not Exists");
                }
                break;
            case 6:
                Console.WriteLine("Program End Successfully");
                break;
            default:
                Console.WriteLine("Please Enter Valid Choies");
                break;
        }

        Console.WriteLine();
        if (switchNum != 6)
        {
            goto start;
        }
    }

    private static bool CheckDestinationTableExists()
    {
        Connection.Open();

        // Create a SqlCommand object with the SQL query and connection
        using SqlCommand command = new SqlCommand(checkDestinationTable, Connection);
        // Execute the SQL query and store the result in a variable
        int tableExists = (int)command.ExecuteScalar();
        Connection.Close();

        // Use the variable as needed
        if (tableExists == 1)
        {
            // The table exists
            return true;
        }

        return false;
    }

    private static bool CheckSourceTableExists()
    {
       
            Connection.Open();
            using SqlCommand command = new SqlCommand(checkSourceTable, Connection);
            int tableExists = (int)command.ExecuteScalar();
            Connection.Close();
            if (tableExists == 1)
            {
                return true;
            }
            return false;
    }


    private static void CreateSourceTable()
    {
        Connection.Open();
        SqlCommand sqlCommand = new SqlCommand(createSourceTableQuery, Connection);
        sqlCommand.ExecuteNonQuery();
        
        DataTable tbl = new DataTable();
        tbl.Columns.Add(new DataColumn("FirstNumber", typeof(Int32)));  
        tbl.Columns.Add(new DataColumn("SecondNumber", typeof(Int32)));  
      
        for(int i=0; i<totalData; i++)  
        {   
            DataRow dr = tbl.NewRow();
            dr["FirstNumber"] = i;
            dr["SecondNumber"] = i * 2;
            tbl.Rows.Add(dr);   
        }  
        
        SqlBulkCopy objbulk = new SqlBulkCopy(Connection);
        objbulk.DestinationTableName = "SourceTable";  
        
        
        objbulk.ColumnMappings.Add("FirstNumber", "FirstNumber");   
        objbulk.ColumnMappings.Add("SecondNumber", "SecondNumber");
        
        objbulk.WriteToServer(tbl);
        Connection.Close();
        
    }
    private static void CreateDestinationTable(int startNumber,int endNumber)
    {
        Connection.Open();
        SqlCommand sqlCommand = new SqlCommand(createDestinationTableQuery, Connection);
        sqlCommand.ExecuteNonQuery();
        _currentIndex = startNumber;
        
        while (_currentIndex<=endNumber)
        {
            List<Record> migrationRecords = new List<Record>();
            int temp=_currentIndex;
            for (int i = 0; i < batchSize && _currentIndex <= endNumber; i++)
            {
                Record record = CheckRecordInSourceTable(_currentIndex);
                migrationRecords.Add(record);
                _currentIndex++;
            }
            

            if (migrationRecords.Count > 0)
            {
                Stopwatch timer = new Stopwatch();
                timer.Start();
                
                
                // for each 100 batch it take about 13 ms to add data using sql bulk
                
                DataTable tbl = new DataTable();
                tbl.Columns.Add(new DataColumn("Fkey", typeof(Int32)));  
                tbl.Columns.Add(new DataColumn("Sum", typeof(Int32)));
                
                foreach (var item in migrationRecords)
                {
                    DataRow dr = tbl.NewRow();
                    dr["Fkey"] = item.Id;
                    dr["Sum"] = item.Sum;
                    tbl.Rows.Add(dr);
                    _completed++;
                }
                SqlBulkCopy objbulk = new SqlBulkCopy(Connection);
                objbulk.DestinationTableName = "DestinationTable";  
                
                
                objbulk.ColumnMappings.Add("Fkey", "Fkey");   
                objbulk.ColumnMappings.Add("Sum", "Sum");
                
                objbulk.WriteToServer(tbl);
                
                
                // for each 100 batch it take about 25 ms to add data using loop
                // SqlCommand command = new SqlCommand("INSERT INTO DestinationTable (Fkey, Sum) VALUES (@Fkey, @Sum)", Connection);
                // command.Parameters.AddWithValue("@Fkey", 0);
                // command.Parameters.AddWithValue("@Sum", 0);
                // foreach (Record record in migrationRecords)
                // {
                //     command.Parameters["@Fkey"].Value = record.Id;
                //     command.Parameters["@Sum"].Value = record.Sum;
                //     command.ExecuteNonQuery();
                //     _completed++;
                // }
                //
                // timer.Stop();
                // var elapsed = timer.ElapsedMilliseconds;
                // Console.WriteLine($"Concat elapsed: {elapsed} ms");
            }

            Console.WriteLine($"total number of record added is : {_completed} from {temp} to {_currentIndex-1}");
          
        }
        Connection.Close();
    }

  
    static Record CheckRecordInSourceTable(int id)
    {
        Record record = null;
        
        SqlCommand command = new SqlCommand($"SELECT * FROM SourceTable WHERE ID = {id}", Connection);

         SqlDataReader reader = command.ExecuteReader();
        if (reader.Read())
        {
            int firstNumber = (int)reader["FirstNumber"];
            int secondNumber = (int)reader["SecondNumber"];

            int sum = CalculateSum(firstNumber,secondNumber);
            record = new Record(id, sum);
        }

        reader.Close();
        return record;
    }
    private static void DropTables()
    {
        Connection.Open();
        SqlCommand command = new SqlCommand("Drop Table DestinationTable,SourceTable",Connection);
        command.ExecuteNonQuery();
        Connection.Close();
    }

    static int CalculateSum(int firstNumber, int secondNumber)
    {
        Thread.Sleep(50);
        return firstNumber + secondNumber;
    }
    
    
    
}