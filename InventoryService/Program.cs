using InventoryService.Data;
using InventoryService.Messaging;
using InventoryService.Models;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddDbContext<InventoryDbContext>(o => o.UseSqlServer(builder.Configuration.GetConnectionString("Default")));

//var bootstrapServers =
//	builder.Configuration["Kafka:BootstrapServers"];

//var topicInitializer =
//	new KafkaTopicInitializer(bootstrapServers!);

//await topicInitializer.InitializeAsync();

//builder.Services.AddHostedService<OrderCreatedConsumer>();
//builder.Services.AddHostedService<OutboxPublisher>();
builder.Services.AddControllers();
builder.Services.AddSwaggerGen();

var app = builder.Build();

await using (var scope = app.Services.CreateAsyncScope())


	await scope.ServiceProvider.GetRequiredService<InventoryDbContext>().Database.MigrateAsync();

if (app.Environment.IsDevelopment())
{
	app.UseSwagger();
	app.UseSwaggerUI();
}

app.UseHttpsRedirection();
app.MapControllers();

app.Run();
