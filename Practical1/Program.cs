
using System.Data;
using System.Data.SqlClient;
using Practical1;

class Program
{
    private static string connectionString = "Data Source=DESKTOP-1VJLI7P;Initial Catalog=practical1db;Integrated Security=True";
    private  static string createSourceTableQuery = "CREATE TABLE SourceTable(ID INT IDENTITY(1,1) PRIMARY KEY, FirstNumber INT, SecondNumber INT)";
    private static string createDestinationTableQuery = "CREATE TABLE DestinationTable(ID INT IDENTITY(1,1) PRIMARY KEY,Fkey INT, Sum INT, FOREIGN KEY (Fkey) REFERENCES SourceTable(ID))";
    static readonly SqlConnection Connection = new (connectionString);
    private static int totalData = 1000;
    private static int batchSize = 100;
    private static int _currentIndex;
    private static int _completed;
    static int endNumber =0;
    static Thread thread;
    static int startNumber;
    private static bool createSourceTable = false;
    private static bool createDestinationTable = false;

    static void Main()
    {
        Connection.Open();
        start:
        Console.WriteLine("1. Create SourceTable\n" +
                          "2. Migrate Data from Source To Destination Table\n" +
                          "3. Status of your Migration\n" +
                          "4. Cancel Migration\n" +
                          "5. Exit");
        
        Console.WriteLine("Enter one choice");
        int switchNum = Convert.ToInt32(Console.ReadLine());
        
        switch (switchNum)
        {
            case 1 :
                if (!createSourceTable)
                {
                    createSourceTable = true;
                    CreateSourceTable();
                }
                else
                {
                    Console.WriteLine("Table is Already Created");
                }
                break;
            case 2:
                if (createSourceTable)
                {
                    Console.WriteLine($"Total Number of record in source table is {totalData} how many you want to Migrate");
                    Console.WriteLine("Enter a startNumber : ");
                    startNumber = Convert.ToInt32(Console.ReadLine());

                    Console.WriteLine("Enter a endNumber : ");
                    endNumber = Convert.ToInt32(Console.ReadLine());
                    thread = new Thread((() => CreateDestinationTable(startNumber,endNumber)));
                    thread.Start();
                }
                else
                {
                    Console.WriteLine("Please create Source Table First");
                }
                break;
            case 3 :
                if (createSourceTable && createDestinationTable)
                {
                    Console.WriteLine($"Total number of data to insert is {endNumber - startNumber+1}\n " +
                                      $"Data added in Destination is {_completed}\n" +
                                      $"Data that left is {endNumber - startNumber - _completed+1}\n");
                }
                else
                {
                    Console.WriteLine("first create tables");
                }
                break;
            case 4:
                thread.Interrupt();
                break;
            case 5: 
                break;
            default:
                Console.WriteLine("Please Enter Valid Choies");
                break;
        }

        Console.WriteLine();
        if (switchNum != 5)
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
        Connection.Close();
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

    static int CalculateSum(int firstNumber, int secondNumber)
    {
        Thread.Sleep(50);
        return firstNumber + secondNumber;
    }
    
}



