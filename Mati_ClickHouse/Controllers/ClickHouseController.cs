using System.Diagnostics;

using Microsoft.AspNetCore.Mvc;

using Octonica.ClickHouseClient;

namespace Mati_ClickHouse.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ClickHouseController : ControllerBase
    {
        private readonly ILogger<WeatherForecastController> _logger;

        public ClickHouseController(ILogger<WeatherForecastController> logger)
        {
            _logger = logger;
        }

        [HttpGet(Name = "ClickHouseClient")]
        public async Task<ActionResult<double>> Get(CancellationToken cancellationToken = default)
        {
            var sb = new ClickHouseConnectionStringBuilder
            {
                Host = "127.0.0.1",
                Port = 9000,
                User = "username",
                Password = "password",
                //Database = "mati_database",
            };
            //using var conn = new ClickHouseConnection("Host=127.0.0.1; Port=8123;");
            using var conn = new ClickHouseConnection(sb);
            await conn.OpenAsync(cancellationToken);

            using var createDatabaseCommand = conn.CreateCommand("CREATE DATABASE IF NOT EXISTS mati_database");
            await createDatabaseCommand.ExecuteNonQueryAsync(cancellationToken);

            using var createTableCommand = conn.CreateCommand("CREATE TABLE IF NOT EXISTS mati_database.vehiclepoint(id Int32, timestamp DateTime, name String, longitude Float32) ENGINE = MergeTree() PRIMARY KEY (id, timestamp)");
            await createTableCommand.ExecuteNonQueryAsync(cancellationToken);

            List<int> ids = Enumerable.Range(1, 13).ToList();
            List<DateTime> timestamps = ids.Select(i => DateTime.Now).ToList();
            List<string> names = ids.Select(i => $"Name #{i}").ToList();
            List<float> longitudes = ids.Select(i => float.Parse(i.ToString())).ToList();

            await using var writer = await conn.CreateColumnWriterAsync("INSERT INTO mati_database.vehiclepoint(id, timestamp, name, longitude) VALUES", cancellationToken);
            await writer.WriteTableAsync(new object[] { ids, timestamps, names, longitudes }, ids.Count, cancellationToken);

            var sw = Stopwatch.StartNew();

            var getdata =
                await conn
                    .CreateCommand("SELECT * FROM mati_database.vehiclepoint")
                    .ExecuteScalarAsync(cancellationToken)
            ;

            sw.Stop();

            var totalSeconds = sw.Elapsed.TotalMilliseconds / 1000.2;

            _logger.LogInformation("{totalSeconds}", totalSeconds);

            return totalSeconds;
        }
    }
}
