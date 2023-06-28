using System.Collections.ObjectModel;

using Dapper;

using Mati_ClickHouse.Models;

using Octonica.ClickHouseClient;

namespace Mati_ClickHouse.Extensions
{
    public static class ClickHouseExtension
    {
        public static ClickHouseConnection ConnectToDatase(this ClickHouseConnectionStringBuilder connectionStringBuilder)
        {
            ClickHouseConnection connection = new(connectionStringBuilder);

            connection.Open();

            return connection;
        }

        public static async ValueTask<ClickHouseConnection> ConnectToDataseAsync(this ClickHouseConnectionStringBuilder connectionStringBuilder, CancellationToken cancellationToken = default)
        {
            ClickHouseConnection connection = new(connectionStringBuilder);

            await connection.OpenAsync(cancellationToken);

            return connection;
        }

        public static void ExecuteCustomNonQuery(this ClickHouseConnection connection, string commandText)
        {
            using var command = connection.CreateCommand(commandText);

            command.ExecuteNonQuery();
        }

        public static async ValueTask ExecuteCustomNonQueryAsync(this ClickHouseConnection connection, string commandText, CancellationToken cancellationToken = default)
        {
            await using var command = connection.CreateCommand(commandText);

            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        public static void InsertBulkData(this ClickHouseConnection connection, object[] objects, int rowCount, string commandText)
        {
            using var writer = connection.CreateColumnWriter(commandText);

            writer.WriteTable(objects, rowCount);

            writer.EndWrite();
        }

        public static async ValueTask InsertBulkDataAsync(this ClickHouseConnection connection, object[] objects, int rowCount, string commandText, CancellationToken cancellationToken = default)
        {
            await using var writer = await connection.CreateColumnWriterAsync(commandText, cancellationToken);

            await writer.WriteTableAsync(objects, rowCount, cancellationToken);

            await writer.EndWriteAsync(cancellationToken);
        }

        public static ICollection<Person> PersonToListFromReader(this ClickHouseConnection connection, string commandText)
        {
            ICollection<Person> list = new Collection<Person>();

            using var cmd = connection.CreateCommand(commandText);

            using var reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                var id = reader.GetFieldValue<int>(0);

                list.Add(new Person(id));
            }

            return list;
        }

        public static async ValueTask<ICollection<Person>> PersonToListFromReaderAsync(this ClickHouseConnection connection, string commandText, CancellationToken cancellationToken = default)
        {
            ICollection<Person> list = new Collection<Person>();

            await using var cmd = connection.CreateCommand(commandText);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                var id = await reader.GetFieldValueAsync<int>(0, cancellationToken);

                list.Add(new Person(id));
            }

            return list;
        }


        public static ICollection<Person> PersonToListFromDapper(this ClickHouseConnection connection, string sql)
        {
            var list = connection.Query<Person>(sql).ToList();

            return list;
        }

        public static async ValueTask<ICollection<Person>> PersonToListFromDapperAsync(this ClickHouseConnection connection, string sql)
        {
            var list = (await connection.QueryAsync<Person>(sql)).ToList();

            return list;
        }
    }
}
