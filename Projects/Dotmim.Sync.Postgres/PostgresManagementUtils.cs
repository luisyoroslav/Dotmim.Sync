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
            var command = "SELECT tbl.table_name AS TableName, tbl.table_schema AS SchemaName FROM information_schema.tables AS tbl " +
                          "WHERE tbl.table_schema NOT IN ('pg_catalog', 'information_schema') " +
                          "AND tbl.table_type = 'BASE TABLE';";

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

            var commandColumn = "SELECT table_catalog, table_schema, table_name name, column_name, ordinal_position, column_default, is_nullable, " +
                                "data_type, character_maximum_length, character_octet_length, numeric_precision, numeric_precision_radix, " +
                                "numeric_scale, datetime_precision, interval_type, interval_precision, character_set_catalog, character_set_schema, " + 
                                "character_set_name, collation_catalog, collation_schema, collation_name, domain_catalog, domain_schema, domain_name, " +
                                "udt_catalog, udt_schema, udt_name, scope_catalog, scope_schema, scope_name, maximum_cardinality, dtd_identifier, " +
                                "is_self_referencing, is_identity, identity_generation, identity_start, identity_increment, identity_maximum, identity_minimum, " +
                                "identity_cycle, is_generated, generation_expression, is_updatable " +
                                "FROM information_schema.columns " +
                                "WHERE table_schema = @schemaName " +
                                "AND table_name = @tableName;";

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
