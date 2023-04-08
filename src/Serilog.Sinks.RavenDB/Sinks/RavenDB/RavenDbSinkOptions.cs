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
using Raven.Client.Documents;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.RavenDB
{
    /// <summary>
    /// Provides RavenDBSink with configurable options
    /// </summary>
    public class RavenDbSinkOptions: PeriodicBatchingSinkOptions
    {
        private TimeSpan? _expiration;
        private TimeSpan? _errorExpiration;

        /// <summary></summary>
        public RavenDbSinkOptions()
        {
            BatchSizeLimit = 50;
        }

        /// <summary>A documentstore for a RavenDB database.</summary>
        public IDocumentStore DocumentStore { get; set; }

        /// <summary>Supplies culture-specific formatting information, or null.</summary>
        public IFormatProvider FormatProvider { get; set; }

        /// <summary>Optional database name. If non is provided the default database if used</summary>
        public string DatabaseName { get; set; } = null;

        #region Expiration Options
        /// <summary>Optional time before a logged message will be expired assuming the expiration bundle is installed.
        /// <see cref="System.Threading.Timeout.InfiniteTimeSpan">Timeout.InfiniteTimeSpan</see> (-00:00:00.0010000) means no expiration.
        /// If this is not provided but errorExpiration is, errorExpiration will be used for non-errors too.</summary>
        public TimeSpan? Expiration
        {
            get => _expiration ?? _errorExpiration;
            set => _expiration = value;
        }

        //_errorExpiration = errorExpiration ?? expiration;
        //_expiration = expiration ?? errorExpiration;

        /// <summary>Optional time before a logged error message will be expired assuming the expiration bundle is installed.
        /// <see cref="System.Threading.Timeout.InfiniteTimeSpan">Timeout.InfiniteTimeSpan</see> (-00:00:00.0010000) means no expiration.
        /// If this is not provided but expiration is, expiration will be used for errors too.</summary>
        public TimeSpan? ErrorExpiration
        {
            get => _errorExpiration ?? _expiration;
            set => _errorExpiration = value;
        }

        /// <summary>Optional callback to dynamically determine log expiration based on event properties.
        /// <see cref="System.Threading.Timeout.InfiniteTimeSpan">Timeout.InfiniteTimeSpan</see> (-00:00:00.0010000) means no expiration.
        /// If this is provided, it will be used instead of expiration or errorExpiration.</summary>
        public Func<LogEvent, TimeSpan> LogExpirationCallback { get; set; } = null;
        #endregion
    }
}
