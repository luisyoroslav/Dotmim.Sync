using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace Dotmim.Sync.Postgres
{
    public static class PostgresManagementUtils
    {
        public static async Task<SyncSetup> GetAllTablesAsync(NpgsqlConnection connection, NpgsqlTransaction transaction)
        {
            var command = "SELECT tbl.table_name TableName, tbl.table_schema SchemaName FROM information_schema.tables tbl" +
                          "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')" +
                          "AND table_type = 'BASE TABLE';";

            var syncSetup = new SyncSetup();

            using (var sqlCommand = new NpgsqlCommand(command, connection))
            {
                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (reader.Read())
                    {
                        var tableName = reader.GetString(0);
                        var schemaName = reader.GetString(1) == "public" ? null : reader.GetString(1);
                        var setupTable = new SetupTable(tableName, schemaName);
                        syncSetup.Tables.Add(setupTable);
                    }
                }

                foreach (var setupTable in syncSetup.Tables)
                {
                    var syncTableColumnsList = await GetColumnsForTableAsync(setupTable.TableName, setupTable.SchemaName, connection, transaction).ConfigureAwait(false);

                    foreach (var column in syncTableColumnsList.Rows)
                        setupTable.Columns.Add(column["name"].ToString());
                }


                if (!alreadyOpened)
                    connection.Close();

            }
            return syncSetup;
        }

        public static async Task<SyncTable> GetColumnsForTableAsync(string tableName, string schemaName, NpgsqlConnection connection, NpgsqlTransaction transaction)
        {

            var commandColumn = "SELECT * FROM information_schema.columns" +
                                "WHERE table_schema = @schemaName" +
                                "AND table_name = @tableName";

            var tableNameNormalized = ParserName.Parse(tableName).Unquoted().Normalized().ToString();
            var tableNameString = ParserName.Parse(tableName).ToString();

            var schemaNameString = "public";
            if (!string.IsNullOrEmpty(schemaName))
            {
                schemaNameString = ParserName.Parse(schemaName).ToString();
                schemaNameString = string.IsNullOrWhiteSpace(schemaNameString) ? "public" : schemaNameString;
            }

            var syncTable = new SyncTable(tableNameNormalized);
            using (var sqlCommand = new NpgsqlCommand(commandColumn, connection))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameString);
                sqlCommand.Parameters.AddWithValue("@schemaName", schemaNameString);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                    syncTable.Load(reader);

                if (!alreadyOpened)
                    connection.Close();

            }
            return syncTable;
        }
    }
}
