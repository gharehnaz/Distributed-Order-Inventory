using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Net;
using System.Text.Json;

namespace Test.EndPoint.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	public class TestController : ControllerBase
	{
		private readonly IConfiguration _configuration;
		public TestController(IConfiguration configuration)
		{
			_configuration= configuration;
		}

		[HttpPost("[action]")]
		public async Task<IActionResult> InventoryConcurencyTest()
		{
			var tasks = Enumerable.Range(1, 100)
			.Select(async i =>
		{
			using var client = new HttpClient();

			var response = await client.PostAsJsonAsync(
				"https://localhost:8703/api/Orders",
				new
				{
					ProductId = "3FA85F64-5717-4562-B3FC-2C963F66AFA6",
					Quantity = 1,
					OrderId = Guid.NewGuid()
				});

			return response.StatusCode;
		});

			var results = await Task.WhenAll(tasks);

			Console.WriteLine(
				$"Success: {results.Count(x => x == HttpStatusCode.OK)}");

			Console.WriteLine(
				$"Failed: {results.Count(x => x != HttpStatusCode.OK)}");

			return Ok();

		}

		[HttpPost("[action]")]
		public async Task<IActionResult> TestEventIdempotency()
		{
			var producerConfig = new ProducerConfig
			{
				BootstrapServers =
					_configuration["Kafka:BootstrapServers"],

				EnableIdempotence = true,
				Acks = Acks.All
			};

			using var producer =
				new ProducerBuilder<string, string>(producerConfig)
					.Build();

			Guid eventId = Guid.NewGuid();
			Guid orderId = Guid.NewGuid();

			var orderCreated = new OrderIdempotentTest
			(
				 eventId,
				 orderId,
				 1,
				 1
			);

			for (int i = 0; i < 10; i++)
			{
				await producer.ProduceAsync(
					"orders",
					new Message<string, string>
					{
						Key = orderCreated.OrderId.ToString(),
						Value = JsonSerializer.Serialize(orderCreated)
					});
			}
			return Ok();
		}
	}
}
