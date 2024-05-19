using Integration.Service;

namespace Integration;

public abstract class Program
{
    public static void Main(string[] args)
    {
        var service = new ItemIntegrationService();

        ThreadPool.QueueUserWorkItem(_ => Console.WriteLine(service.SaveItem("a").Message));
        ThreadPool.QueueUserWorkItem(_ => Console.WriteLine(service.SaveItem("b").Message));
        ThreadPool.QueueUserWorkItem(_ => Console.WriteLine(service.SaveItem("c").Message));

        Thread.Sleep(500);

        ThreadPool.QueueUserWorkItem(_ => Console.WriteLine(service.SaveItem("a").Message));
        ThreadPool.QueueUserWorkItem(_ => Console.WriteLine(service.SaveItem("b").Message));
        ThreadPool.QueueUserWorkItem(_ => Console.WriteLine(service.SaveItem("c").Message));

        Thread.Sleep(5000);

        Console.WriteLine("Everything recorded:");

        service.GetAllItems().ForEach(Console.WriteLine);

        Console.ReadLine();
    }
}