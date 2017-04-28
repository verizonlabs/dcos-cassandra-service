package com.mesosphere.dcos.cassandra.common.config;

import org.apache.mesos.config.Configuration;

import java.util.List;

interface ConfigValidation {
    List<ConfigValidationError> validate(Configuration oldConfig, Configuration newConfig);
}
