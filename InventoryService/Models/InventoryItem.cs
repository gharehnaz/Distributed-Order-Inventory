namespace InventoryService.Models;
public sealed class InventoryItem
{
    public Guid Id { get; set; }
    public Guid ProductId { get; set; }
    public int AvailableQuantity { get; set; }
}
