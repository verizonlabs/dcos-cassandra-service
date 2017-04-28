package com.mesosphere.dcos.cassandra.scheduler;

import com.mesosphere.dcos.cassandra.common.config.MutableSchedulerConfiguration;
import io.dropwizard.Application;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import javax.annotation.Nullable;

class CassandraDropwizardAppRule<C extends MutableSchedulerConfiguration> extends DropwizardAppRule {
    private TestingServer server = TestModule.createTestingServerQuietly();
    private CuratorFramework curator = TestModule.createClient(server);

    public CassandraDropwizardAppRule(Class<? extends Application<C>> applicationClass) {
        super(applicationClass);
    }

    public CassandraDropwizardAppRule(Class<? extends Application<C>> applicationClass, @Nullable String configPath, ConfigOverride... configOverrides) {
        super(applicationClass, configPath, configOverrides);
    }

    public CassandraDropwizardAppRule(Class<? extends Application<C>> applicationClass, C configuration) {
        super(applicationClass, configuration);
    }

    public CassandraDropwizardAppRule(DropwizardTestSupport<C> testSupport) {
        super(testSupport);
    }

    @Override
    public MutableSchedulerConfiguration getConfiguration() {
        return (MutableSchedulerConfiguration) super.getConfiguration();
    }

    @Override
    protected void before() {
        try {
            server.start();
            curator.start();
            super.before();
        } catch (Exception e) {

        }
    }

    @Override
    public Application getApplication() {
        return super.getApplication();
    }

    @Override
    protected void after() {
        super.after();
        CloseableUtils.closeQuietly(curator);
        CloseableUtils.closeQuietly(server);
    }

    public TestingServer getServer() {
        return server;
    }

    public void setServer(TestingServer server) {
        this.server = server;
    }

    public CuratorFramework getCurator() {
        return curator;
    }

    public void setCurator(CuratorFramework curator) {
        this.curator = curator;
    }
}
