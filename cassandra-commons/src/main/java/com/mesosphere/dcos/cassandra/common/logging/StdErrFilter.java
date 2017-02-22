package com.mesosphere.dcos.cassandra.common.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

import java.util.Arrays;
import java.util.List;

public class StdErrFilter extends Filter<ILoggingEvent> {

    @Override
    public FilterReply decide(ILoggingEvent event) {
        List<Level> eventsToKeep = Arrays.asList(Level.ERROR);
        if (eventsToKeep.contains(event.getLevel())) {
            return FilterReply.NEUTRAL;
        } else {
            return FilterReply.DENY;
        }
    }
}