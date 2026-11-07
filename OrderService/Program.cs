using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using OrderService.Messaging;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddControllers();
builder.Services.AddDbContext<OrderDbContext>(o => o.UseSqlServer(builder.Configuration.GetConnectionString("Default")));
builder.Services.AddHostedService<InventoryResultConsumer>();
builder.Services.AddHostedService<OutboxPublisher>();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

await using (var scope = app.Services.CreateAsyncScope())
	await scope.ServiceProvider.GetRequiredService<OrderDbContext>().Database.MigrateAsync();
if (app.Environment.IsDevelopment())
{
	app.UseSwagger();
	app.UseSwaggerUI(options =>
	{
		options.SwaggerEndpoint(
			"/swagger/v1/swagger.json",
			"Order API V1");
	});
}

app.UseHttpsRedirection();
app.MapControllers();

app.Run();
