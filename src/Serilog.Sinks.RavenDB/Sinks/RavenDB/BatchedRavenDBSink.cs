// Copyright 2020 Serilog Contributors
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Json;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;
using LogEvent = Serilog.Sinks.RavenDB.Data.LogEvent;

namespace Serilog.Sinks.RavenDB
{
    internal sealed class BatchedRavenDBSink : IBatchedLogEventSink, IDisposable
    {
        private readonly RavenDbSinkOptions _options;
        private readonly bool _disposeDocumentStore;

        /// <summary>
        /// Construct a <see cref="T:Serilog.Sinks.RavenDB.BatchedRavenDBSink" /> posting to the specified database.
        /// </summary>
        /// <param name="options">Options controlling behavior of the sink.</param>
        public BatchedRavenDBSink(RavenDbSinkOptions options)
        {
            if (options.DocumentStore == null) throw new ArgumentNullException(nameof(options.DocumentStore));
            _options = options;
            _disposeDocumentStore = false;

        }
        /// <summary>
        /// Emit a batch of log events, running asynchronously.
        /// </summary>
        /// <param name="batch">The events to emit.</param>
        /// <remarks>Override either <see cref="PeriodicBatchingSink.EmitBatch"/> or <see cref="PeriodicBatchingSink.EmitBatchAsync"/>,
        /// not both.</remarks>
        public Task EmitBatchAsync(IEnumerable<Events.LogEvent> batch)
        {
            switch (_options.StorageMethod)
            {
                case RavenDBSinkStorageMethod.Session:
                    return UseDBSession(batch);
                case RavenDBSinkStorageMethod.BulkInsert:
                    return UseDBBulkInsert(batch);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private async Task UseDBSession(IEnumerable<Events.LogEvent> batch)
        {
            using (var session = string.IsNullOrWhiteSpace(_options.DatabaseName)
                       ? _options.DocumentStore.OpenAsyncSession()
                       : _options.DocumentStore.OpenAsyncSession(_options.DatabaseName))
            {
                foreach (var logEvent in batch)
                {
                    var logEventDoc = new LogEvent(logEvent, logEvent.RenderMessage(_options.FormatProvider));
                    await session.StoreAsync(logEventDoc);

                    var expiration = DetermineExpiration(logEvent);

                    if (expiration == Timeout.InfiniteTimeSpan) continue;
                    var metaData = session.Advanced.GetMetadataFor(logEventDoc);
                    metaData[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.Add(expiration);
                }

                await session.SaveChangesAsync();
            }
        }
        private async Task UseDBBulkInsert(IEnumerable<Events.LogEvent> batch)
        {
            BulkInsertOperation bulkInsert = null;
            try
            {
                bulkInsert = _options.DocumentStore.BulkInsert(_options.DatabaseName);
                foreach (var logEvent in batch)
                {
                    var metadata = new MetadataAsDictionary();
                    var logEventDoc = new LogEvent(logEvent, logEvent.RenderMessage(_options.FormatProvider));
                    var expiration = DetermineExpiration(logEvent);

                    if (expiration != Timeout.InfiniteTimeSpan)
                        metadata.Add(Constants.Documents.Metadata.Expires, DateTime.UtcNow.Add(expiration));
                    await bulkInsert.StoreAsync(logEventDoc, metadata);
                }
            }
            finally
            {
                if (bulkInsert != null)
                {
                    await bulkInsert.DisposeAsync().ConfigureAwait(false);
                }
            }
        }

        private TimeSpan DetermineExpiration(Events.LogEvent logEvent)
        {
            return _options.LogExpirationCallback?.Invoke(logEvent)
                   ?? ((logEvent.Level == LogEventLevel.Error || logEvent.Level == LogEventLevel.Fatal) && _options.ErrorExpiration.HasValue
                       ? _options.ErrorExpiration.Value
                       : _options.Expiration ?? Timeout.InfiniteTimeSpan);
        }

        public Task OnEmptyBatchAsync()
        {
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            if (_disposeDocumentStore)
            {
                _options.DocumentStore.Dispose();
            }
        }
    }
}