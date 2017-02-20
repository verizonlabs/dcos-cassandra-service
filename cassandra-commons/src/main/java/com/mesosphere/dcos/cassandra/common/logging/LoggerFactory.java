package com.mesosphere.dcos.cassandra.common.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.joran.spi.JoranException;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.dropwizard.logging.LoggingFactory;
import io.dropwizard.logging.LoggingUtil;

public class LoggerFactory implements LoggingFactory {

    @JsonIgnore
    private LoggerContext loggerContext;
    @JsonIgnore
    private final ContextInitializer contextInitializer;

    public LoggerFactory() {
        this.loggerContext = LoggingUtil.getLoggerContext();
        this.contextInitializer = new ContextInitializer(loggerContext);
    }

    @Override
    public void configure(MetricRegistry metricRegistry, String name) {
        try {
            contextInitializer.autoConfig();
        } catch (JoranException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        loggerContext.stop();
    }
}