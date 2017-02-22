/*
 * Copyright 2016 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mesosphere.dcos.cassandra.executor.config;


import com.mesosphere.dcos.cassandra.common.logging.LoggerFactory;
import io.dropwizard.Configuration;
import io.dropwizard.logging.LoggingFactory;

/**
 * The Configuration object for the Executor application. It contains no
 * properties and the executor requires no static application
 * level configuration. If, at some point, more configuration is necessary it
 * should be implemented here.
 */
public class CassandraExecutorConfiguration extends Configuration {
    @Override
    public LoggingFactory getLoggingFactory() {
        return new LoggerFactory();
    }
}
