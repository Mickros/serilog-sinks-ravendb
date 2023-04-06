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
        public async Task EmitBatchAsync(IEnumerable<Events.LogEvent> batch)
        {
            using (var session = string.IsNullOrWhiteSpace(_options.DatabaseName) ? _options.DocumentStore.OpenAsyncSession() : _options.DocumentStore.OpenAsyncSession(_options.DatabaseName))
            {
                foreach (var logEvent in batch)
                {
                    var logEventDoc = new LogEvent(logEvent, logEvent.RenderMessage(_options.FormatProvider));
                    await session.StoreAsync(logEventDoc);

                    var expiration =
                        _options.LogExpirationCallback != null ? _options.LogExpirationCallback(logEvent) :
                        _options.Expiration == null ? Timeout.InfiniteTimeSpan :
                        logEvent.Level == LogEventLevel.Error || logEvent.Level == LogEventLevel.Fatal ? _options.ErrorExpiration.Value :
                        _options.Expiration.Value;

                    if (expiration != Timeout.InfiniteTimeSpan)
                    {
                        var metaData = session.Advanced.GetMetadataFor(logEventDoc);
                        metaData[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.Add(expiration);
                    }
                }
                await session.SaveChangesAsync();
            }
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