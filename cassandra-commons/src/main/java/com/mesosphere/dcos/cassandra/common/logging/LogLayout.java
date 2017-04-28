package com.mesosphere.dcos.cassandra.common.logging;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.LayoutBase;
import org.slf4j.MDC;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Map;

public class LogLayout extends LayoutBase<ILoggingEvent> {

    private PatternLayout patternLayout;

    public LogLayout() {
        Map<String, String> envs = System.getenv();
        for (Map.Entry<String, String> env : envs.entrySet()) {
            MDC.put("ENV:" + env.getKey(), env.getValue());
        }
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        String pid = rt.getName();
        if (pid.contains("@")) {
            pid = pid.substring(0, pid.indexOf("@"));
        }
        MDC.put("PID", pid);

        PatternLayout.defaultConverterMap.put("level", LogLevelConverter.class.getName());
        PatternLayout.defaultConverterMap.put("le", LogLevelConverter.class.getName());
        PatternLayout.defaultConverterMap.put("p", LogLevelConverter.class.getName());
    }

    @Override
    public void start() {
        super.start();
        patternLayout.start();
    }

    @Override
    public void stop() {
        super.stop();
        patternLayout.stop();
    }

    public void setPattern(String value) {
        patternLayout = new PatternLayout();
        patternLayout.setPattern(value);
        patternLayout.setContext(getContext());

    }

    @Override
    public String doLayout(ILoggingEvent event) {
        return patternLayout.doLayout(event);
    }
}