using Dotmim.Sync.Builders;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Postgres.Builders
{
    public class NpgsqlBuilder : DbBuilder
    {
        public override async Task EnsureDatabaseAsync(DbConnection connection, DbTransaction transaction = null)
        {
            // Chek if db exists
            var exists = await NpgsqlManagementUtils.DatabaseExistsAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction).ConfigureAwait(false);
            
            if (!exists)
                throw new MissingDatabaseException(connection.Database);
        }

        public override Task<SyncTable> EnsureTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => Task.FromResult(new SyncTable(tableName, schemaName));

        public override Task<SyncSetup> GetAllTablesAsync(DbConnection connection, DbTransaction transaction = null)
            => NpgsqlManagementUtils.GetAllTablesAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction);

        public override async Task<(string DatabaseName, string Version)> GetHelloAsync(DbConnection connection, DbTransaction transaction = null)
        {
            return await NpgsqlManagementUtils.GetHelloAsync(connection as NpgsqlConnection, transaction as NpgsqlTransaction).ConfigureAwait(false);
        }

        public override Task<SyncTable> GetTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => NpgsqlManagementUtils.GetTableAsync(tableName, schemaName, connection as NpgsqlConnection, transaction as NpgsqlTransaction);

        public override Task<SyncTable> GetTableDefinitionAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => NpgsqlManagementUtils.GetTableDefinitionAsync(tableName, schemaName, connection as NpgsqlConnection, transaction as NpgsqlTransaction);
        
        public override Task<bool> ExistsTableAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null)
            => NpgsqlManagementUtils.TableExistsAsync(tableName, schemaName, connection as NpgsqlConnection, transaction as NpgsqlTransaction);

        public override Task<SyncTable> GetTableColumnsAsync(string tableName, string schemaName, DbConnection connection, DbTransaction transaction = null) =>
            NpgsqlManagementUtils.GetColumnsForTableAsync(tableName, schemaName, connection as NpgsqlConnection, transaction as NpgsqlTransaction);

        public override Task DropsTableIfExistsAsync(string tableName, string schemaName, DbConnection connection,
            DbTransaction transaction = null) =>
            throw new NotImplementedException();

        public override Task RenameTableAsync(string tableName, string schemaName, string newTableName, string newSchemaName,
            DbConnection connection, DbTransaction transaction = null) =>
            throw new NotImplementedException(); // todo
    }
}
