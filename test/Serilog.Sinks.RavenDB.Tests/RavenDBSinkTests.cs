﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Serilog.Events;
using Serilog.Parsing;
using Xunit;
using LogEvent = Serilog.Sinks.RavenDB.Data.LogEvent;

namespace Serilog.Sinks.RavenDB.Tests
{
    public class RavenDBSinkTests
    {
        static RavenDBSinkTests()
        {
            Raven.Embedded.EmbeddedServer.Instance.StartServer();
        }

        [Fact]
        public void WhenAnEventIsWrittenToTheSinkUsingSessionStorageItIsRetrievableFromTheDocumentStore()
        {
            const string databaseName = nameof(WhenAnEventIsWrittenToTheSinkUsingSessionStorageItIsRetrievableFromTheDocumentStore);
            using var documentStore = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();

            try
            {
                var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
                var exception = new ArgumentException("Mládek");
                const LogEventLevel level = LogEventLevel.Information;
                const string messageTemplate = "{Song}++";
                var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = documentStore
                };

                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                using var session = documentStore.OpenSession();
                var events = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).ToList();
                Assert.Single(events);
                var single = events.Single();
                Assert.Equal(messageTemplate, single.MessageTemplate);
                Assert.Equal("\"New Macabre\"++", single.RenderedMessage);
                Assert.Equal(timestamp, single.Timestamp);
                Assert.Equal(level, single.Level);
                Assert.Equal(1, single.Properties.Count);
                Assert.Equal("New Macabre", single.Properties["Song"]);

                // BUG Exception Deserializing fails and does not reproduce an object equivalent to the one stored in the DB
                //Assert.Equal(exception.Message, single.Exception.Message);
            }
            finally
            {
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }

        [Fact]
        public void WhenADatabaseNameIsProvidedItIsUsedWithSessionStorage()
        {
            const string databaseName = nameof(WhenADatabaseNameIsProvidedItIsUsedWithSessionStorage);
            const string customDB = "NamedDB";
            using var documentStore = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();

            try
            {
                var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
                var exception = new ArgumentException("Mládek");
                const LogEventLevel level = LogEventLevel.Information;
                const string messageTemplate = "{Song}++";
                var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = documentStore,
                    DatabaseName = customDB
                };
                Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(customDB);


                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                using (var session = documentStore.OpenSession())
                {
                    var events = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).ToList();
                    Assert.Empty(events);
                }

                using (var session = documentStore.OpenSession(customDB))
                {
                    var events = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).ToList();
                    Assert.Single(events);
                }
            }
            finally
            {
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(customDB, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }

        [Fact]
        public void WhenAnEventIsWrittenWithExpirationUsingSessionStorageItHasProperMetadata()
        {
            const string databaseName = nameof(WhenAnEventIsWrittenWithExpirationUsingSessionStorageItHasProperMetadata);
            using var documentStore = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();

            try
            {
                var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
                var expiration = TimeSpan.FromDays(1);
                var errorExpiration = TimeSpan.FromMinutes(15);
                var targetExpiration = DateTime.UtcNow.Add(expiration);
                var exception = new ArgumentException("Mládek");
                const LogEventLevel level = LogEventLevel.Information;
                const string messageTemplate = "{Song}++";
                var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = documentStore,
                    Expiration = expiration,
                    ErrorExpiration = errorExpiration
                };


                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                using (var session = documentStore.OpenSession())
                {
                    var logEvent = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).First();
                    var metaData = session.Advanced.GetMetadataFor(logEvent)[Constants.Documents.Metadata.Expires].ToString();
                    var actualExpiration = Convert.ToDateTime(metaData).ToUniversalTime();
                    Assert.True(actualExpiration >= targetExpiration, $"The document should expire on or after {targetExpiration} but expires {actualExpiration}");
                }
            }
            finally
            {
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }
 
        [Fact]
        public void WhenAnEventIsWrittenToTheSinkUsingBulkInsertStorageItIsRetrievableFromTheDocumentStore()
        {
            const string databaseName = nameof(WhenAnEventIsWrittenToTheSinkUsingBulkInsertStorageItIsRetrievableFromTheDocumentStore);
            using var documentStore = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();

            try
            {
                var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
                var exception = new ArgumentException("Mládek");
                const LogEventLevel level = LogEventLevel.Information;
                const string messageTemplate = "{Song}++";
                var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = documentStore,
                    StorageMethod = RavenDBSinkStorageMethod.BulkInsert
                };

                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                using var session = documentStore.OpenSession();
                var events = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).ToList();
                Assert.Single(events);
                var single = events.Single();
                Assert.Equal(messageTemplate, single.MessageTemplate);
                Assert.Equal("\"New Macabre\"++", single.RenderedMessage);
                Assert.Equal(timestamp, single.Timestamp);
                Assert.Equal(level, single.Level);
                Assert.Equal(1, single.Properties.Count);
                Assert.Equal("New Macabre", single.Properties["Song"]);

                // BUG Exception Deserializing fails and does not reproduce an object equivalent to the one stored in the DB
                //Assert.Equal(exception.Message, single.Exception.Message);
            }
            finally
            {
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }

        [Fact]
        public void WhenADatabaseNameIsProvidedItIsUsedWithBulkInsertStorage()
        {
            const string databaseName = nameof(WhenADatabaseNameIsProvidedItIsUsedWithBulkInsertStorage);
            const string customDB = "NamedDB";
            using var documentStore = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();

            try
            {
                var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
                var exception = new ArgumentException("Mládek");
                const LogEventLevel level = LogEventLevel.Information;
                const string messageTemplate = "{Song}++";
                var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = documentStore,
                    DatabaseName = customDB,
                    StorageMethod = RavenDBSinkStorageMethod.BulkInsert
                };

                documentStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(customDB)));
                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                using (var session = documentStore.OpenSession())
                {
                    var events = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).ToList();
                    Assert.Empty(events);
                }

                using (var session = documentStore.OpenSession(customDB))
                {
                    var events = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).ToList();
                    Assert.Single(events);
                }
            }
            finally
            {
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(customDB, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }

        [Fact]
        public void WhenAnEventIsWrittenWithExpirationUsingBulkInsertStorageItHasProperMetadata()
        {
            const string databaseName = nameof(WhenAnEventIsWrittenWithExpirationUsingBulkInsertStorageItHasProperMetadata);
            using var documentStore = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();

            try
            {
                var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
                var expiration = TimeSpan.FromDays(1);
                var errorExpiration = TimeSpan.FromMinutes(15);
                var targetExpiration = DateTime.UtcNow.Add(expiration);
                var exception = new ArgumentException("Mládek");
                const LogEventLevel level = LogEventLevel.Information;
                const string messageTemplate = "{Song}++";
                var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = documentStore,
                    Expiration = expiration,
                    ErrorExpiration = errorExpiration,
                    StorageMethod = RavenDBSinkStorageMethod.BulkInsert
                };


                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                using (var session = documentStore.OpenSession())
                {
                    var logEvent = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).First();
                    var metaData = session.Advanced.GetMetadataFor(logEvent)[Constants.Documents.Metadata.Expires].ToString();
                    var actualExpiration = Convert.ToDateTime(metaData).ToUniversalTime();
                    Assert.True(actualExpiration >= targetExpiration, $"The document should expire on or after {targetExpiration} but expires {actualExpiration}");
                }
            }
            finally
            {
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }

        [Fact]
        public void WhenAnEventIsWrittenWithExpirationCallbackItHasProperMetadata()
        {
            const string databaseName = nameof(WhenAnErrorEventIsWrittenWithExpirationItHasProperMetadata);
            using var documentStore = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();

            try
            {
                var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
                var expiration = TimeSpan.FromDays(1);
                var errorExpiration = TimeSpan.FromMinutes(15);
                var targetExpiration = DateTime.UtcNow.Add(expiration);
                TimeSpan Func(Events.LogEvent le) => le.Level == LogEventLevel.Information ? expiration : errorExpiration;
                var exception = new ArgumentException("Ml�dek");
                const LogEventLevel level = LogEventLevel.Information;
                const string messageTemplate = "{Song}++";
                var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = documentStore,
                    LogExpirationCallback = Func
                };

                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                using (var session = documentStore.OpenSession())
                {
                    var logEvent = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).First();
                    var metaData = session.Advanced.GetMetadataFor(logEvent)[Constants.Documents.Metadata.Expires].ToString();
                    var actualExpiration = Convert.ToDateTime(metaData).ToUniversalTime();
                    Assert.True(actualExpiration >= targetExpiration, $"The document should expire on or after {targetExpiration} but expires {actualExpiration}");
                }
            }
            finally
            {
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }

        [Fact]
        public void WhenAnErrorEventIsWrittenWithExpirationItHasProperMetadata()
        {
            const string databaseName = nameof(WhenAnErrorEventIsWrittenWithExpirationItHasProperMetadata);
            using var documentStore = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();

            try
            {
                var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
                var errorExpiration = TimeSpan.FromDays(1);
                var expiration = TimeSpan.FromMinutes(15);
                var targetExpiration = DateTime.UtcNow.Add(errorExpiration);
                var exception = new ArgumentException("Mládek");
                const LogEventLevel level = LogEventLevel.Error;
                const string messageTemplate = "{Song}++";
                var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = documentStore,
                    Expiration = expiration,
                    ErrorExpiration = errorExpiration
                };

                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                using (var session = documentStore.OpenSession())
                {
                    var logEvent = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).First();
                    var metaData = session.Advanced.GetMetadataFor(logEvent)[Constants.Documents.Metadata.Expires].ToString();
                    var actualExpiration = Convert.ToDateTime(metaData).ToUniversalTime();
                    Assert.True(actualExpiration >= targetExpiration, $"The document should expire on or after {targetExpiration} but expires {actualExpiration}");
                }
            }
            finally
            {
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }

        [Fact]
        public void WhenAFatalEventIsWrittenWithExpirationItHasProperMetadata()
        {
            const string databaseName = nameof(WhenAFatalEventIsWrittenWithExpirationItHasProperMetadata);
            using var documentStore = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();

            try
            {
                var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
                var errorExpiration = TimeSpan.FromDays(1);
                var expiration = TimeSpan.FromMinutes(15);
                var targetExpiration = DateTime.UtcNow.Add(errorExpiration);
                var exception = new ArgumentException("Mládek");
                const LogEventLevel level = LogEventLevel.Fatal;
                const string messageTemplate = "{Song}++";
                var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = documentStore,
                    Expiration = expiration,
                    ErrorExpiration = errorExpiration
                };

                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                using (var session = documentStore.OpenSession())
                {
                    var logEvent = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).First();
                    var metaData = session.Advanced.GetMetadataFor(logEvent)[Constants.Documents.Metadata.Expires].ToString();
                    var actualExpiration = Convert.ToDateTime(metaData).ToUniversalTime();
                    Assert.True(actualExpiration >= targetExpiration, $"The document should expire on or after {targetExpiration} but expires {actualExpiration}");
                }
            }
            finally
            {
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }

        [Fact]
        public void WhenNoErrorExpirationSetBuExpirationSetUseExpirationForErrors()
        {
            const string databaseName = nameof(WhenNoErrorExpirationSetBuExpirationSetUseExpirationForErrors);
            using var documentStore = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();

            try
            {
                var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
                var expiration = TimeSpan.FromMinutes(15);
                var targetExpiration = DateTime.UtcNow.Add(expiration);
                var exception = new ArgumentException("Mládek");
                const LogEventLevel level = LogEventLevel.Fatal;
                const string messageTemplate = "{Song}++";
                var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = documentStore,
                    Expiration = expiration
                };

                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                using (var session = documentStore.OpenSession())
                {
                    var logEvent = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).First();
                    var metaData = session.Advanced.GetMetadataFor(logEvent)[Constants.Documents.Metadata.Expires].ToString();
                    var actualExpiration = Convert.ToDateTime(metaData).ToUniversalTime();
                    Assert.True(actualExpiration >= targetExpiration, $"The document should expire on or after {targetExpiration} but expires {actualExpiration}");
                }

            }
            finally
            {
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }

        [Fact]
        public void WhenNoExpirationSetBuErrorExpirationSetUseErrorExpirationForMessages()
        {
            const string databaseName = nameof(WhenNoExpirationSetBuErrorExpirationSetUseErrorExpirationForMessages);
            using var documentStore = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();

            try
            {
                var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
                var errorExpiration = TimeSpan.FromMinutes(15);
                var targetExpiration = DateTime.UtcNow.Add(errorExpiration);
                var exception = new ArgumentException("Mládek");
                const LogEventLevel level = LogEventLevel.Information;
                const string messageTemplate = "{Song}++";
                var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = documentStore,
                    ErrorExpiration = errorExpiration
                };

                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                using (var session = documentStore.OpenSession())
                {
                    var logEvent = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).First();
                    var metaData = session.Advanced.GetMetadataFor(logEvent)[Constants.Documents.Metadata.Expires].ToString();
                    var actualExpiration = Convert.ToDateTime(metaData).ToUniversalTime();
                    Assert.True(actualExpiration >= targetExpiration, $"The document should expire on or after {targetExpiration} but expires {actualExpiration}");
                }
            }
            finally
            {
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }

        [Fact]
        public void WhenNoExpirationIsProvidedMessagesDontExpire()
        {
            const string databaseName = nameof(WhenNoExpirationIsProvidedMessagesDontExpire);
            using var documentStore = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();
            try
            {
                var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
                var exception = new ArgumentException("Mládek");
                const LogEventLevel level = LogEventLevel.Error;
                const string messageTemplate = "{Song}++";
                var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = documentStore
                };

                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                using (var session = documentStore.OpenSession())
                {
                    var logEvent = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).First();
                    Assert.False(session.Advanced.GetMetadataFor(logEvent).ContainsKey(Constants.Documents.Metadata.Expires), "No expiration set");
                }

            }
            finally
            {
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }

        [Fact]
        public void WhenExpirationSetToInfiniteMessagesDontExpire()
        {
            const string databaseName = nameof(WhenExpirationSetToInfiniteMessagesDontExpire);
            using var documentStore = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();

            try
            {
                var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
                var expiration = Timeout.InfiniteTimeSpan;
                var exception = new ArgumentException("Mládek");
                const LogEventLevel level = LogEventLevel.Information;
                const string messageTemplate = "{Song}++";
                var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = documentStore,
                    Expiration = expiration
                };

                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                using (var session = documentStore.OpenSession())
                {
                    var logEvent = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).First();
                    Assert.False(session.Advanced.GetMetadataFor(logEvent).ContainsKey(Constants.Documents.Metadata.Expires), "No expiration set");
                }
            }
            finally
            {
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }

        [Fact]
        public void WhenErrorExpirationSetToInfiniteErrorsDontExpire()
        {
            const string databaseName = nameof(WhenErrorExpirationSetToInfiniteErrorsDontExpire);
            using var documentStore = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();

            try
            {
                var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
                var errorExpiration = Timeout.InfiniteTimeSpan;
                var exception = new ArgumentException("Mládek");
                const LogEventLevel level = LogEventLevel.Error;
                const string messageTemplate = "{Song}++";
                var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = documentStore,
                    ErrorExpiration = errorExpiration
                };

                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                using (var session = documentStore.OpenSession())
                {
                    var logEvent = session.Query<LogEvent>().Customize(x => x.WaitForNonStaleResults()).First();
                    Assert.False(session.Advanced.GetMetadataFor(logEvent).ContainsKey(Constants.Documents.Metadata.Expires), "No expiration set");
                }
            }
            finally
            {
                documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }

        [Fact]
        public void WhenUsingConnectionStringInCtorInternalDocumentStoreIsCreated()
        {
            var timestamp = new DateTimeOffset(2013, 05, 28, 22, 10, 20, 666, TimeSpan.FromHours(10));
            var exception = new ArgumentException("Mládek");
            const LogEventLevel level = LogEventLevel.Information;
            const string messageTemplate = "{Song}++";
            var properties = new List<LogEventProperty> { new LogEventProperty("Song", new ScalarValue("New Macabre")) };
            var events = new Dictionary<string, LogEvent>();

            const string databaseName = nameof(WhenUsingConnectionStringInCtorInternalDocumentStoreIsCreated);
            using var store = Raven.Embedded.EmbeddedServer.Instance.GetDocumentStore(databaseName);
            store.OnBeforeStore += (_, e) => events[e.DocumentId] = (LogEvent)e.Entity;
            store.Initialize();

            try
            {
                var options = new RavenDbSinkOptions
                {
                    DocumentStore = store
                };

                using (var ravenSink = new BatchedRavenDBSink(options))
                {
                    var template = new MessageTemplateParser().Parse(messageTemplate);
                    var logEvent = new Events.LogEvent(timestamp, level, exception, template, properties);
                    ravenSink.EmitBatchAsync(new[] { logEvent }).Wait();
                }

                Assert.Single(events);
                var single = events.First().Value;
                Assert.Equal(messageTemplate, single.MessageTemplate);
                Assert.Equal("\"New Macabre\"++", single.RenderedMessage);
                Assert.Equal(timestamp, single.Timestamp);
                Assert.Equal(level, single.Level);
                Assert.Equal(1, single.Properties.Count);
                Assert.Equal("New Macabre", single.Properties["Song"]);
                Assert.Equal(exception.Message, single.Exception.Message);
            }
            finally
            {
                store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: null, timeToWaitForConfirmation: null));
            }
        }
    }
}
