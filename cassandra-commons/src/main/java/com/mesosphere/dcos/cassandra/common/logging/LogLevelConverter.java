package com.mesosphere.dcos.cassandra.common.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

public class LogLevelConverter extends ClassicConverter {

    private static final String ALARM_LEVEL = "ALARM";
    private static final String ERROR_LEVEL = "ERROR";
    private static final String INFO_LEVEL = "INFO";
    private static final String DEBUG_LEVEL = "DEBUG";

    @Override
    public String convert(ILoggingEvent event) {
        int level = event.getLevel().toInt();
        switch (level) {
            case Level.ERROR_INT:
                return ERROR_LEVEL;
            case Level.DEBUG_INT:
                return DEBUG_LEVEL;
            case Level.INFO_INT:
                return INFO_LEVEL;
            case Level.TRACE_INT:
                return DEBUG_LEVEL;
            case Level.WARN_INT:
                return INFO_LEVEL;
        }
        throw new IllegalArgumentException("Invalid level type [" + event.getLevel().toString() + "] found.");
    }
}
