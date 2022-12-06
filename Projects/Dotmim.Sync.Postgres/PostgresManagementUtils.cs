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

            var commandColumn = $"Select col.name as name, " +
                                $"col.column_id,  " +
                                $"typ.name as [type],  " +
                                $"col.max_length,  " +
                                $"col.precision,  " +
                                $"col.scale,  " +
                                $"col.is_nullable,  " +
                                $"col.is_computed,  " +
                                $"col.is_identity,  " +
                                $"ind.is_unique,  " +
                                $"ident_seed(sch.name + '.' + tbl.name) AS seed, " +
                                $"ident_incr(sch.name + '.' + tbl.name) AS step, " +
                                $"object_definition(col.default_object_id) AS defaultvalue " +
                                $"  from sys.columns as col " +
                                $"  Inner join sys.tables as tbl on tbl.object_id = col.object_id " +
                                $"  Inner join sys.schemas as sch on tbl.schema_id = sch.schema_id " +
                                $"  Inner Join sys.systypes typ on typ.xusertype = col.system_type_id " +
                                $"  Left outer join sys.indexes ind on ind.object_id = col.object_id and ind.index_id = col.column_id " +
                                $"  Where tbl.name = @tableName and sch.name = @schemaName ";

            var tableNameNormalized = ParserName.Parse(tableName).Unquoted().Normalized().ToString();
            var tableNameString = ParserName.Parse(tableName).ToString();

            var schemaNameString = "dbo";
            if (!string.IsNullOrEmpty(schemaName))
            {
                schemaNameString = ParserName.Parse(schemaName).ToString();
                schemaNameString = string.IsNullOrWhiteSpace(schemaNameString) ? "dbo" : schemaNameString;
            }

            var syncTable = new SyncTable(tableNameNormalized);
            using (var sqlCommand = new SqlCommand(commandColumn, connection))
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
