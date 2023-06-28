using System.Diagnostics;
using System.Text;

using Mati_ClickHouse.Extensions;

using Microsoft.AspNetCore.Mvc;

using Octonica.ClickHouseClient;

namespace Mati_ClickHouse.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ClickHouseController : ControllerBase
    {
        private readonly ILogger<ClickHouseController> _logger;

        public ClickHouseController(ILogger<ClickHouseController> logger)
        {
            _logger = logger;
        }

        [HttpGet(Name = "ClickHouseClientSync")]
        public ActionResult<string> Get()
        {
            {
                var clickHouseConnectionStringBuilder = new ClickHouseConnectionStringBuilder
                {
                    Host = "127.0.0.1",
                    Port = 9000,
                    User = "username",
                    Password = "password",
                    Database = "SinjulMati",
                };

                using var connection = clickHouseConnectionStringBuilder.ConnectToDatase();

                var dataBaseName = "SinjulMati";

                var tableName = "GoTooshee";

                var sb = new StringBuilder();

                {
                    connection.ExecuteCustomNonQuery($"CREATE DATABASE IF NOT EXISTS {dataBaseName}");

                    connection.ExecuteCustomNonQuery($"CREATE TABLE IF NOT EXISTS {tableName}(id Int32) ENGINE = MergeTree() PRIMARY KEY (id)");
                }

                {
                    var sw = Stopwatch.StartNew();

                    var rowCount = 10_000_000;

                    List<int> ids = Enumerable.Range(1, rowCount).ToList();

                    var objects = new object[] { ids };

                    connection.InsertBulkData(objects, rowCount, $"INSERT INTO {tableName} VALUES");

                    sw.Stop();

                    var totalSeconds = sw.Elapsed.TotalMilliseconds / 1000.2;

                    var message = $"InsertBulkData TotalSeconds: {totalSeconds}and TotalRecords: {rowCount}\t|";

                    sb.Append(message);
                }

                {
                    var sw = Stopwatch.StartNew();

                    var list = connection.PersonToListFromReader($"SELECT * FROM {tableName}");

                    sw.Stop();

                    var totalSeconds = sw.Elapsed.TotalMilliseconds / 1000.2;

                    var message = $"Fetch100mRecordFromReader TotalSeconds: {totalSeconds}and TotalRecords: {list.Count}\t|";

                    sb.Append(message);
                }

                {
                    var sw = Stopwatch.StartNew();

                    var list = connection.PersonToListFromDapper($"SELECT * FROM {tableName}");

                    sw.Stop();

                    var totalSeconds = sw.Elapsed.TotalMilliseconds / 1000.2;

                    var message = $"Fetch100mRecordFromDapper TotalSeconds: {totalSeconds}and TotalRecords: {list.Count}\t|";

                    sb.Append(message);
                }

                connection.Close();

                var textDataResult = sb.ToString();

                _logger.LogInformation("{textDataResult}", textDataResult);

                return textDataResult;
            }
        }


        [HttpPost(Name = "ClickHouseClientASync")]
        public async Task<ActionResult<string>> Post(CancellationToken cancellationToken = default)
        {
            {
                var clickHouseConnectionStringBuilder = new ClickHouseConnectionStringBuilder
                {
                    Host = "127.0.0.1",
                    Port = 9000,
                    User = "username",
                    Password = "password",
                    Database = "SinjulMati",
                };

                using var connection = await clickHouseConnectionStringBuilder.ConnectToDataseAsync(cancellationToken);

                var dataBaseName = "SinjulMati";

                var tableName = "GoTooshee";

                var sb = new StringBuilder();

                {
                    await connection.ExecuteCustomNonQueryAsync($"CREATE DATABASE IF NOT EXISTS {dataBaseName}", cancellationToken);

                    await connection.ExecuteCustomNonQueryAsync($"CREATE TABLE IF NOT EXISTS {tableName}(id Int32) ENGINE = MergeTree() PRIMARY KEY (id)", cancellationToken);
                }

                {
                    var sw = Stopwatch.StartNew();

                    var rowCount = 10_000_000;

                    List<int> ids = Enumerable.Range(1, rowCount).ToList();

                    var objects = new object[] { ids };

                    await connection.InsertBulkDataAsync(objects, rowCount, $"INSERT INTO {tableName} VALUES", cancellationToken);

                    sw.Stop();

                    var totalSeconds = sw.Elapsed.TotalMilliseconds / 1000.2;

                    var message = $"InsertBulkData TotalSeconds: {totalSeconds}and TotalRecords: {rowCount}\t|";

                    sb.Append(message);
                }

                {
                    var sw = Stopwatch.StartNew();

                    var list = await connection.PersonToListFromReaderAsync($"SELECT * FROM {tableName}", cancellationToken);

                    sw.Stop();

                    var totalSeconds = sw.Elapsed.TotalMilliseconds / 1000.2;

                    var message = $"Fetch100mRecordFromReader TotalSeconds: {totalSeconds}and TotalRecords: {list.Count}\t|";

                    sb.Append(message);
                }

                {
                    var sw = Stopwatch.StartNew();

                    var list = await connection.PersonToListFromDapperAsync($"SELECT * FROM {tableName}");

                    sw.Stop();

                    var totalSeconds = sw.Elapsed.TotalMilliseconds / 1000.2;

                    var message = $"Fetch100mRecordFromDapper TotalSeconds: {totalSeconds}and TotalRecords: {list.Count}\t|";

                    sb.Append(message);
                }

                connection.Close();

                var textDataResult = sb.ToString();

                _logger.LogInformation("{textDataResult}", textDataResult);

                return textDataResult;
            }
        }
    }
}
