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

using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.RavenDB
{
    /// <summary>
    /// Writes log events as documents to a RavenDB database.
    /// </summary>
    public class RavenDBSink : PeriodicBatchingSink
    {
        /// <summary>
        /// Construct a <see cref="T:Serilog.Sinks.RavenDB.RavenDBSink" /> posting to the specified database.
        /// </summary>
        /// <param name="options">Options controlling behavior of the sink.</param>
        public RavenDBSink(RavenDbSinkOptions options) : this(new BatchedRavenDBSink(options), options)
        {
        }

        private RavenDBSink(IBatchedLogEventSink batchedSink, PeriodicBatchingSinkOptions options) : base(batchedSink, options)
        {
        }
    }
}
