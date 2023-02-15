namespace Practical1;

public class Record
{
    public int Id { get; set; }
    public int Sum { get; set; }

    public Record(int id, int sum)
    {
        Id = id;
        Sum = sum;
    }
}