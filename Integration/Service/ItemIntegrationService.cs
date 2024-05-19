using Integration.Common;
using Integration.Backend;
using StackExchange.Redis;

namespace Integration.Service;

public sealed class ItemIntegrationService
{
    private static readonly ConnectionMultiplexer RedisConnection = ConnectionMultiplexer.Connect("localhost:6379");
    private static readonly IDatabase RedisDatabase = RedisConnection.GetDatabase();

    //This is a dependency that is normally fulfilled externally.
    private ItemOperationBackend ItemIntegrationBackend { get; set; } = new();

    // This is called externally and can be called multithreaded, in parallel.
    // More than one item with the same content should not be saved. However,
    // calling this with different contents at the same time is OK, and should
    // be allowed for performance reasons.
    public Result SaveItem(string itemContent)
    {
        string lockKey = $"lock:{itemContent}";
        bool lockAcquired = RedisDatabase.LockTake(lockKey, Environment.MachineName, TimeSpan.FromSeconds(30));

        // Check the backend to see if the content is already saved.
        if (!lockAcquired)
        {
            return new Result(false, $"Duplicate item received with content {itemContent}.");
        }

        try
        {
            // Check the backend to see if the content is already saved.
            if (ItemIntegrationBackend.FindItemsWithContent(itemContent).Count != 0)
            {
                return new Result(false, $"Duplicate item received with content {itemContent}.");
            }

            var item = ItemIntegrationBackend.SaveItem(itemContent);

            return new Result(true, $"Item with content {itemContent} saved with id {item.Id}");
        }
        finally
        {
            RedisDatabase.LockRelease(lockKey, Environment.MachineName);
        }
    }

    public List<Item> GetAllItems()
    {
        return ItemIntegrationBackend.GetAllItems();
    }
}