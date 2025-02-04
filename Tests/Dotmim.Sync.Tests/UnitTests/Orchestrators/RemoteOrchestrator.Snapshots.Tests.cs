﻿using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests.UnitTests
{
    public partial class RemoteOrchestratorTests : IDisposable
    {
 

        [Fact]
        public async Task RemoteOrchestrator_CreateSnapshot_CheckInterceptors()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_srv");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var serverProvider = new SqlSyncProvider(cs);

            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, serverProvider), true, true);
            await ctx.Database.EnsureCreatedAsync();

            var scopeName = "scopesnap1";
            var onSnapshotCreating = false;
            var onSnapshotCreated = false;

            // snapshot directory
            var snapshotDirctoryName = HelperDatabase.GetRandomName();
            var snapshotDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), snapshotDirctoryName);

            var options = new SyncOptions
            {
                SnapshotsDirectory = snapshotDirectory,
                BatchSize = 200
            };

            var setup = new SyncSetup(Tables);

            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);


            remoteOrchestrator.OnSnapshotCreating(args =>
            {
                Assert.IsType<SnapshotCreatingArgs>(args);
                Assert.Equal(SyncStage.SnapshotCreating, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.Equal(ConnectionState.Closed, args.Connection.State);
                Assert.NotNull(args.Schema);
                Assert.Equal(snapshotDirectory, args.SnapshotDirectory);
                Assert.NotEqual(0, args.Timestamp);

                onSnapshotCreating = true;
            });
            remoteOrchestrator.OnSnapshotCreated(args =>
            {
                Assert.IsType<SnapshotCreatedArgs>(args);
                Assert.Equal(SyncStage.ChangesSelecting, args.Context.SyncStage);
                Assert.Equal(scopeName, args.Context.ScopeName);
                Assert.NotNull(args.Connection);
                Assert.Null(args.Transaction);
                Assert.NotNull(args.BatchInfo);

                var finalDirectoryFullName = Path.Combine(snapshotDirectory, scopeName);

                Assert.Equal(finalDirectoryFullName, args.BatchInfo.DirectoryRoot);
                Assert.Equal("ALL", args.BatchInfo.DirectoryName);
                Assert.NotEmpty(args.BatchInfo.BatchPartsInfo);
                Assert.Equal(16, args.BatchInfo.BatchPartsInfo.Count);
                // Get last batch 
                var lastBatch = args.BatchInfo.BatchPartsInfo.First(bi => bi.Index == 15);

                Assert.True(lastBatch.IsLastBatch);

                onSnapshotCreated = true;
            });

            var bi = await remoteOrchestrator.CreateSnapshotAsync(scopeName, setup);

            Assert.True(onSnapshotCreating);
            Assert.True(onSnapshotCreated);


            var dbNameCli = HelperDatabase.GetRandomName("tcp_lo_cli");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbNameCli, true);

            var csClient = HelperDatabase.GetConnectionString(ProviderType.Sql, dbNameCli);
            var clientProvider = new SqlSyncProvider(csClient);


            // Make a first sync to be sure everything is in place
            var agent = new SyncAgent(clientProvider, serverProvider, options);

            var onSnapshotApplying = false;
            var onSnapshotApplied = false;



            agent.LocalOrchestrator.OnSnapshotApplying(saa =>
            {
                onSnapshotApplying = true;
            });

            agent.LocalOrchestrator.OnSnapshotApplied(saa =>
            {
                onSnapshotApplied = true;
            });


            // Making a first sync, will initialize everything we need
            await agent.SynchronizeAsync(scopeName, setup);

            Assert.True(onSnapshotApplying);
            Assert.True(onSnapshotApplied);



            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task RemoteOrchestrator_CreateSnapshot_CheckBatchInfo()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, true);
            await ctx.Database.EnsureCreatedAsync();

            var rowsCount = GetServerDatabaseRowsCount((dbName, ProviderType.Sql, sqlProvider));

            var scopeName = "scopesnap2";

            // snapshot directory
            var snapshotDirctoryName = HelperDatabase.GetRandomName();
            var snapshotDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), snapshotDirctoryName);

            var options = new SyncOptions
            {
                SnapshotsDirectory = snapshotDirectory,
                BatchSize = 200
            };

            var setup = new SyncSetup(Tables);
            var provider = new SqlSyncProvider(cs);

            var orchestrator = new RemoteOrchestrator(provider, options);

            var bi = await orchestrator.CreateSnapshotAsync(scopeName, setup);

            var finalDirectoryFullName = Path.Combine(snapshotDirectory, scopeName);

            Assert.NotNull(bi);
            Assert.Equal(finalDirectoryFullName, bi.DirectoryRoot);
            Assert.Equal("ALL", bi.DirectoryName);
            Assert.NotEmpty(bi.BatchPartsInfo);
            Assert.Equal(16, bi.BatchPartsInfo.Count);

            // Get last batch 
            var lastBatch = bi.BatchPartsInfo.First(bi => bi.Index == 15);
                
            Assert.True(lastBatch.IsLastBatch);
            Assert.Equal(rowsCount, bi.RowsCount);

            // Check summary.json exists.
            var summaryFile = Path.Combine(bi.GetDirectoryFullPath(), "summary.json");
            var summaryString = new StreamReader(summaryFile).ReadToEnd();
            var summaryObject = JObject.Parse(summaryString);

            Assert.NotNull(summaryObject);
            string summaryDirname = (string)summaryObject["dirname"];
            Assert.NotNull(summaryDirname);
            Assert.Equal("ALL", summaryDirname);

            string summaryDir = (string)summaryObject["dir"];
            Assert.NotNull(summaryDir);
            Assert.Equal(finalDirectoryFullName, summaryDir);

            Assert.NotEmpty(summaryObject["parts"]);
            Assert.Equal(16, summaryObject["parts"].Count());

            Assert.NotNull(summaryObject["parts"][0]["file"]);
            Assert.NotNull(summaryObject["parts"][0]["index"]);
            Assert.NotNull(summaryObject["parts"][0]["last"]);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        [Fact]
        public async Task RemoteOrchestrator_CreateSnapshot_WithParameters_CheckBatchInfo()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, true);
            await ctx.Database.EnsureCreatedAsync();

            // snapshot directory
            var snapshotDirctoryName = HelperDatabase.GetRandomName();
            var snapshotDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDirectory(), snapshotDirctoryName);

            var options = new SyncOptions
            {
                SnapshotsDirectory = snapshotDirectory,
                BatchSize = 200
            };

            var setup = new SyncSetup(Tables);
            var provider = new SqlSyncProvider(cs);

            setup.Filters.Add("Customer", "CompanyName");

            var addressCustomerFilter = new SetupFilter("CustomerAddress");
            addressCustomerFilter.AddParameter("CompanyName", "Customer");
            addressCustomerFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressCustomerFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(addressCustomerFilter);

            var addressFilter = new SetupFilter("Address");
            addressFilter.AddParameter("CompanyName", "Customer");
            addressFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "AddressId", "Address", "AddressId");
            addressFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            addressFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(addressFilter);

            var orderHeaderFilter = new SetupFilter("SalesOrderHeader");
            orderHeaderFilter.AddParameter("CompanyName", "Customer");
            orderHeaderFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderHeaderFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            orderHeaderFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(orderHeaderFilter);

            var orderDetailsFilter = new SetupFilter("SalesOrderDetail");
            orderDetailsFilter.AddParameter("CompanyName", "Customer");
            orderDetailsFilter.AddJoin(Join.Left, "SalesOrderHeader").On("SalesOrderDetail", "SalesOrderID", "SalesOrderHeader", "SalesOrderID");
            orderDetailsFilter.AddJoin(Join.Left, "CustomerAddress").On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderDetailsFilter.AddJoin(Join.Left, "Customer").On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            orderDetailsFilter.AddWhere("CompanyName", "Customer", "CompanyName");
            setup.Filters.Add(orderDetailsFilter);


            var orchestrator = new RemoteOrchestrator(provider, options);

            SyncParameters parameters = new SyncParameters();
            var p1 = new SyncParameter("CompanyName", "A Bike Store");
            parameters.Add(p1);

            var bi = await orchestrator.CreateSnapshotAsync(setup, parameters);

            var finalDirectoryFullName = Path.Combine(snapshotDirectory, SyncOptions.DefaultScopeName);

            Assert.NotNull(bi);
            Assert.Equal(finalDirectoryFullName, bi.DirectoryRoot);
            Assert.Equal("CompanyName_ABikeStore", bi.DirectoryName);
            Assert.NotEmpty(bi.BatchPartsInfo);
            Assert.Equal(16, bi.BatchPartsInfo.Count);

            // Get last batch 
            var lastBatch = bi.BatchPartsInfo.First(bi => bi.Index == 15);

            Assert.True(lastBatch.IsLastBatch);

            // Check summary.json exists.
            var summaryFile = Path.Combine(bi.GetDirectoryFullPath(), "summary.json");
            var summaryString = new StreamReader(summaryFile).ReadToEnd();
            var summaryObject = JObject.Parse(summaryString);

            Assert.NotNull(summaryObject);
            string summaryDirname = (string)summaryObject["dirname"];
            Assert.NotNull(summaryDirname);
            Assert.Equal("CompanyName_ABikeStore", summaryDirname);

            string summaryDir = (string)summaryObject["dir"];
            Assert.NotNull(summaryDir);
            Assert.Equal(finalDirectoryFullName, summaryDir);

            Assert.NotEmpty(summaryObject["parts"]);
            Assert.Equal(16, summaryObject["parts"].Count());

            Assert.NotNull(summaryObject["parts"][0]["file"]);
            Assert.NotNull(summaryObject["parts"][0]["index"]);
            Assert.NotNull(summaryObject["parts"][0]["last"]);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }



        [Fact]
        public async Task RemoteOrchestrator_CreateSnapshot_ShouldFail_If_MissingMandatoriesOptions()
        {
            var dbName = HelperDatabase.GetRandomName("tcp_lo_");
            await HelperDatabase.CreateDatabaseAsync(ProviderType.Sql, dbName, true);
            var cs = HelperDatabase.GetConnectionString(ProviderType.Sql, dbName);
            var sqlProvider = new SqlSyncProvider(cs);
            var ctx = new AdventureWorksContext((dbName, ProviderType.Sql, sqlProvider), true, true);
            await ctx.Database.EnsureCreatedAsync();

            var scopeName = "scopesnap";

            // snapshot directory
            var snapshotDirctoryName = HelperDatabase.GetRandomName();
            var snapshotDirectory = Path.Combine(Environment.CurrentDirectory, snapshotDirctoryName);

            var options = new SyncOptions();

            var setup = new SyncSetup(Tables);
            var provider = new SqlSyncProvider(cs);

            var orchestrator = new RemoteOrchestrator(provider, options);
            var se = await Assert.ThrowsAsync<SyncException>(() => orchestrator.CreateSnapshotAsync(scopeName, setup));

            Assert.Equal(SyncStage.SnapshotCreating, se.SyncStage);
            Assert.Equal("SnapshotMissingMandatariesOptionsException", se.TypeName);

            options = new SyncOptions { BatchSize = 2000 };
            orchestrator = new RemoteOrchestrator(provider, options);
            se = await Assert.ThrowsAsync<SyncException>(() => orchestrator.CreateSnapshotAsync(scopeName, setup));

            Assert.Equal(SyncStage.SnapshotCreating, se.SyncStage);
            Assert.Equal("SnapshotMissingMandatariesOptionsException", se.TypeName);

            options = new SyncOptions { };
            orchestrator = new RemoteOrchestrator(provider, options);
            se = await Assert.ThrowsAsync<SyncException>(() => orchestrator.CreateSnapshotAsync(scopeName, setup));

            Assert.Equal(SyncStage.SnapshotCreating, se.SyncStage);
            Assert.Equal("SnapshotMissingMandatariesOptionsException", se.TypeName);

            HelperDatabase.DropDatabase(ProviderType.Sql, dbName);
        }


        public int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t)
        {
            int totalCountRows = 0;

            using (var serverDbCtx = new AdventureWorksContext(t))
            {
                totalCountRows += serverDbCtx.Address.Count();
                totalCountRows += serverDbCtx.Customer.Count();
                totalCountRows += serverDbCtx.CustomerAddress.Count();
                totalCountRows += serverDbCtx.Employee.Count();
                totalCountRows += serverDbCtx.EmployeeAddress.Count();
                totalCountRows += serverDbCtx.Log.Count();
                totalCountRows += serverDbCtx.Posts.Count();
                totalCountRows += serverDbCtx.PostTag.Count();
                totalCountRows += serverDbCtx.PricesList.Count();
                totalCountRows += serverDbCtx.PricesListCategory.Count();
                totalCountRows += serverDbCtx.PricesListDetail.Count();
                totalCountRows += serverDbCtx.Product.Count();
                totalCountRows += serverDbCtx.ProductCategory.Count();
                totalCountRows += serverDbCtx.ProductModel.Count();
                totalCountRows += serverDbCtx.SalesOrderDetail.Count();
                totalCountRows += serverDbCtx.SalesOrderHeader.Count();
                //totalCountRows += serverDbCtx.Sql.Count();
                totalCountRows += serverDbCtx.Tags.Count();
            }

            return totalCountRows;
        }

    }
}
