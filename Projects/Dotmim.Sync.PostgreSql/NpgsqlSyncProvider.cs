using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using System.Data.Common;
using Npgsql;
using System;
using Dotmim.Sync.Postgres.Scope;
using Dotmim.Sync.Postgres.Builders;
using Dotmim.Sync.Postgres.Manager;

namespace Dotmim.Sync.Postgres
{

    public class NpgsqlSyncProvider : CoreProvider
    {
        private DbMetadata dbMetadata;
        private NpgsqlConnectionStringBuilder builder;


        public NpgsqlSyncProvider() : base()
        { }

        public NpgsqlSyncProvider(string connectionString) : base()
        {
            this.ConnectionString = connectionString;

            if (!string.IsNullOrEmpty(this.ConnectionString))
            {
                this.builder = new NpgsqlConnectionStringBuilder(this.ConnectionString);
            }
        }

        public NpgsqlSyncProvider(NpgsqlConnectionStringBuilder builder) : base()
        {
            if (string.IsNullOrEmpty(builder.ConnectionString))
                throw new Exception("You have to provide parameters to the Sql builder to be able to construct a valid connection string.");

            this.builder = builder;
            this.ConnectionString = builder.ConnectionString;
        }

        static string providerType;
        public override string GetProviderTypeName() => ProviderType;
        public static string ProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(providerType))
                    return providerType;

                var type = typeof(NpgsqlSyncProvider);
                providerType = $"{type.Name}, {type}";

                return providerType;
            }

        }

        static string shortProviderType;
        public override string GetShortProviderTypeName() => ShortProviderType;
        public static string ShortProviderType
        {
            get
            {
                if (!string.IsNullOrEmpty(shortProviderType))
                    return shortProviderType;

                var type = typeof(NpgsqlSyncProvider);
                shortProviderType = type.Name;

                return shortProviderType;
            }
        }
        public override string GetDatabaseName()
        {
            if (builder != null && !String.IsNullOrEmpty(builder.Database))
                return builder.Database;

            return string.Empty;
        }

        public override DbMetadata GetMetadata()
        {
            if (this.dbMetadata == null)
                this.dbMetadata = new NpgsqlDbMetadata();

            return this.dbMetadata;
        }
        
        public override bool ShouldRetryOn(Exception exception)
        {
            if (exception is NpgsqlException)
                return ((NpgsqlException)exception).IsTransient;

            return true;
        }

        public override (ParserName tableName, ParserName trackingName) GetParsers(SyncTable tableDescription, SyncSetup setup = null) => throw new NotImplementedException();

        public override void EnsureSyncException(SyncException syncException)
        {
            if (!string.IsNullOrEmpty(this.ConnectionString))
            {
                var builder = new NpgsqlConnectionStringBuilder(this.ConnectionString);

                syncException.DataSource = builder.Host;
                syncException.InitialCatalog = builder.Database;
            }

            // Can add more info from SqlException
            var sqlException = syncException.InnerException as NpgsqlException;

            if (sqlException == null) return;

            syncException.Number = sqlException.ErrorCode;
        }

        public override bool CanBeServerProvider => true;

        public override DbConnection CreateConnection() => new NpgsqlConnection(this.ConnectionString);
        public override DbScopeBuilder GetScopeBuilder(string scopeInfoTableName) => new NpgsqlScopeBuilder(scopeInfoTableName);
        public override DbTableBuilder GetTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
        {
            //var tableBuilder = new NpgsqlTableBuilder(tableDescription, setup)
            //{
            //    UseBulkProcedures = this.SupportBulkOperations,
            //    UseChangeTracking = this.UseChangeTracking,
            //    Filter = tableDescription.GetFilter()
            //};

            //return tableBuilder;
            return null; // todo
        }

        public override DbSyncAdapter GetSyncAdapter(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup, string scopeName)
        {
            //return new NpgsqlSyncAdapter(tableName, setup);
            return null; // todo
        }

        public override DbBuilder GetDatabaseBuilder() => new NpgsqlBuilder();

    }
}
