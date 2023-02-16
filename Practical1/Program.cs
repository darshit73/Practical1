using System.Data;
using System.Data.SqlClient;
namespace Practical1;

class Program
{
    private static readonly string connectionString = "Data Source=DESKTOP-1VJLI7P;Initial Catalog=practical1db;Integrated Security=True";
    private  static readonly string createSourceTableQuery = "CREATE TABLE SourceTable(ID INT IDENTITY(1,1) PRIMARY KEY, FirstNumber INT, SecondNumber INT)";
    private static readonly string createDestinationTableQuery = "CREATE TABLE DestinationTable(ID INT IDENTITY(1,1) PRIMARY KEY,Fkey INT, Sum INT, FOREIGN KEY (Fkey) REFERENCES SourceTable(ID))";
    private static readonly SqlConnection Connection = new (connectionString);
    private static readonly int totalData = 1000;
    private static readonly int batchSize = 100;
    private static int _currentIndex;
    private static int _completed;
    private static int _endNumber ;
    private static Thread thread;
    private static int _startNumber;
    private static bool _createSourceTable;
    private static bool _createDestinationTable;

    static void Main()
    {
        Connection.Open();
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
                    thread = new Thread((() => CreateDestinationTable(_startNumber,_endNumber)));
                    thread.Start();
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
                    thread.Interrupt();
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
                    DropSuccessfully();
                    Console.WriteLine("Table Drop Successfully");
                }
                else
                {
                    Console.WriteLine("Table Dose Not Exists");
                }
                break;
            case 6:
                Console.WriteLine("Program End Successfully");
                Connection.Close();
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

  

    private static void CreateSourceTable()
    {
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
        
        
    }
    private static void CreateDestinationTable(int startNumber,int endNumber)
    {
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
                if (record != null)
                {
                    migrationRecords.Add(record);
                    _currentIndex++;
                }
            }
            

            if (migrationRecords.Count > 0)
            {
                SqlCommand command = new SqlCommand("INSERT INTO DestinationTable (Fkey, Sum) VALUES (@Fkey, @Sum)", Connection);
                command.Parameters.AddWithValue("@Fkey", 0);
                command.Parameters.AddWithValue("@Sum", 0);
                foreach (Record record in migrationRecords)
                {
                    command.Parameters["@Fkey"].Value = record.Id;
                    command.Parameters["@Sum"].Value = record.Sum;
                    command.ExecuteNonQuery();
                    _completed++;
                }
            }

            Console.WriteLine($"total number of record added is : {_completed} from {temp} to {_currentIndex-1}");
          
        }
        // Connection.Close();
    }

  
    static Record CheckRecordInSourceTable(int id)
    {
        Record record = null;
        
        SqlCommand command = new SqlCommand($"SELECT * FROM SourceTable WHERE ID = {id}", Connection);

        using (SqlDataReader reader = command.ExecuteReader())
        {
            if (reader.Read())
            {
                int firstNumber = (int)reader["FirstNumber"];
                int secondNumber = (int)reader["SecondNumber"];

                int sum = CalculateSum(firstNumber,secondNumber);
                record = new Record(id, sum);
            }
        }
           
          
        return record;
    }
    private static void DropSuccessfully()
    {
        SqlCommand command = new SqlCommand("Drop Table DestinationTable,SourceTable",Connection);
        command.ExecuteNonQuery();
    }

    static int CalculateSum(int firstNumber, int secondNumber)
    {
        Thread.Sleep(50);
        return firstNumber + secondNumber;
    }
    
}