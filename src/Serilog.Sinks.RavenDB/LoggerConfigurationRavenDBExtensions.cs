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
        public static LoggerConfiguration RavenDB(
            this LoggerSinkConfiguration loggerConfiguration,
            RavenDbSinkOptions options,
            LogEventLevel restrictedToMinimumLevel = LogEventLevel.Verbose,
            LoggingLevelSwitch levelSwitch = null)
        {
            if (loggerConfiguration == null) throw new ArgumentNullException(nameof(loggerConfiguration));
            
            return loggerConfiguration.Sink(new RavenDBSink(options), restrictedToMinimumLevel, levelSwitch);
        }
    }
}
