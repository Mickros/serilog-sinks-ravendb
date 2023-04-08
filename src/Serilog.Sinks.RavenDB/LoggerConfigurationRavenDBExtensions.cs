using System;
using Raven.Client.Documents;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Sinks.RavenDB;

// Copyright 2014 Serilog Contributors
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

namespace Serilog
{
    /// <summary>
    /// Adds the WriteTo.RavenDB() extension method to <see cref="LoggerConfiguration"/>.
    /// </summary>
    public static class LoggerConfigurationRavenDBExtensions
    {
        /// <summary>
        /// Adds a sink that writes log events as documents to a RavenDB database.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="options">Options controlling behavior of the sink.</param>
        /// <param name="restrictedToMinimumLevel">The minimum level for events passed through the sink. Ignored when <paramref name="levelSwitch" /> is specified.</param>
        /// <param name="levelSwitch">A switch allowing the pass-through minimum level to be changed at runtime.</param>
        /// <returns>Configuration object allowing method chaining.</returns>
        /// <exception cref="ArgumentNullException">A required parameter is null.</exception>
        // ReSharper disable once MemberCanBePrivate.Global
        public static LoggerConfiguration RavenDB(
            this LoggerSinkConfiguration loggerConfiguration,
            RavenDbSinkOptions options,
            LogEventLevel restrictedToMinimumLevel = LogEventLevel.Verbose,
            LoggingLevelSwitch levelSwitch = null)
        {
            if (loggerConfiguration == null) throw new ArgumentNullException(nameof(loggerConfiguration));
            
            return loggerConfiguration.Sink(new RavenDBSink(options), restrictedToMinimumLevel, levelSwitch);
        }

        /// <summary>
        /// Adds a sink that writes log events as documents to a RavenDB database.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="restrictedToMinimumLevel">The minimum level for events passed through the sink. Ignored when <paramref name="levelSwitch" /> is specified.</param>
        /// <param name="levelSwitch">A switch allowing the pass-through minimum level to be changed at runtime.</param>
        /// <param name="batchSizeLimit"></param>
        /// <param name="period"></param>
        /// <param name="queueLimit"></param>
        /// <param name="eagerlyEmitFirstEvent"></param>
        /// 
        /// <param name="documentStore">A documentstore for a RavenDB database.</param>
        /// <param name="formatProvider">Supplies culture-specific formatting information, or null.</param>
        /// <param name="databaseName">Optional database name. If non is provided the default database if used</param>
        /// <param name="storageMethod">Defines how Log event is send to the Database</param>
        /// <param name="logExpirationCallback">Optional callback to dynamically determine log expiration based on event properties.
        /// <see cref="System.Threading.Timeout.InfiniteTimeSpan">Timeout.InfiniteTimeSpan</see> (-00:00:00.0010000) means no expiration.
        /// If this is provided, it will be used instead of expiration or errorExpiration.</param>
        /// <param name="errorExpiration">Optional time before a logged error message will be expired assuming the expiration bundle is installed.
        /// <see cref="System.Threading.Timeout.InfiniteTimeSpan">Timeout.InfiniteTimeSpan</see> (-00:00:00.0010000) means no expiration.
        /// If this is not provided but expiration is, expiration will be used for errors too.</param>
        /// <param name="expiration">Optional time before a logged message will be expired assuming the expiration bundle is installed.
        /// <see cref="System.Threading.Timeout.InfiniteTimeSpan">Timeout.InfiniteTimeSpan</see> (-00:00:00.0010000) means no expiration.
        /// If this is not provided but errorExpiration is, errorExpiration will be used for non-errors too.</param>
        /// <returns>Configuration object allowing method chaining.</returns>
        /// <exception cref="ArgumentNullException">A required parameter is null.</exception>
        public static LoggerConfiguration RavenDB(
            this LoggerSinkConfiguration loggerConfiguration,
            LogEventLevel restrictedToMinimumLevel = LogEventLevel.Verbose,
            LoggingLevelSwitch levelSwitch = null,
            int? batchSizeLimit = null,
            TimeSpan? period = null,
            int? queueLimit = null,
            bool? eagerlyEmitFirstEvent = null,

            IDocumentStore documentStore = null,
            IFormatProvider formatProvider = null,
            string databaseName = null,
            RavenDBSinkStorageMethod? storageMethod = null,
            Func<LogEvent, TimeSpan> logExpirationCallback = null,
            TimeSpan? errorExpiration = null,
            TimeSpan? expiration = null
            )
        {
            var options = new RavenDbSinkOptions();

            if (batchSizeLimit != null) options.BatchSizeLimit = batchSizeLimit.Value;
            if (period != null) options.Period = period.Value;
            if (queueLimit != null) options.QueueLimit = queueLimit.Value;
            if (eagerlyEmitFirstEvent != null) options.EagerlyEmitFirstEvent = eagerlyEmitFirstEvent.Value;

            if (documentStore != null) options.DocumentStore = documentStore;
            if (formatProvider != null) options.FormatProvider = formatProvider;
            if (databaseName != null) options.DatabaseName = databaseName;
            if (logExpirationCallback != null) options.LogExpirationCallback = logExpirationCallback;
            if (errorExpiration != null) options.ErrorExpiration = errorExpiration.Value;
            if (expiration != null) options.Expiration = expiration.Value;
            if (storageMethod != null) options.StorageMethod = storageMethod.Value;

            return RavenDB(loggerConfiguration, options, restrictedToMinimumLevel, levelSwitch);
        }
    }
}
