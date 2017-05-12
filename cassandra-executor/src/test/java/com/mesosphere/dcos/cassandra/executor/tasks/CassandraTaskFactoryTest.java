package com.mesosphere.dcos.cassandra.executor.tasks;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.*;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import io.dropwizard.configuration.*;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.validation.BaseValidator;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.curator.test.TestingServer;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.dcos.Capabilities;
import org.apache.mesos.executor.ExecutorTask;
import org.apache.mesos.executor.ExecutorTaskException;
import org.apache.mesos.executor.ProcessTask;
import org.apache.mesos.state.StateStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

/**
 * Created by gabriel on 9/20/16.
 */
public class CassandraTaskFactoryTest {
    private static CassandraTaskFactory taskFactory;
    private static TestingServer server;
    private static final String testDaemonName = "test-daemon-name";
    private CassandraState cassandraState;

    @Mock
    private ExecutorDriver executorDriver;

    @Before
    public void beforeEach() throws Exception {
        MockitoAnnotations.initMocks(this);
        server = new TestingServer();
        server.start();

        final ConfigurationFactory<MutableSchedulerConfiguration> factory =
                new YamlConfigurationFactory<>(
                        MutableSchedulerConfiguration.class,
                        BaseValidator.newValidator(),
                        Jackson.newObjectMapper().registerModule(
                                new GuavaModule())
                                .registerModule(new Jdk8Module()),
                        "dw");

        MutableSchedulerConfiguration config = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());

        ServiceConfig initial = config.createConfig().getServiceConfig();

        final CassandraSchedulerConfiguration targetConfig = config.createConfig();
        ClusterTaskConfig clusterTaskConfig = targetConfig.getClusterTaskConfig();

        final CuratorFrameworkConfig curatorConfig = config.getCuratorConfig();
        RetryPolicy retryPolicy =
                (curatorConfig.getOperationTimeout().isPresent()) ?
                        new RetryUntilElapsed(
                                curatorConfig.getOperationTimeoutMs()
                                        .get()
                                        .intValue()
                                , (int) curatorConfig.getBackoffMs()) :
                        new RetryForever((int) curatorConfig.getBackoffMs());

        StateStore stateStore = new CuratorStateStore(
                targetConfig.getServiceConfig().getName(),
                server.getConnectString(),
                retryPolicy);
        stateStore.storeFrameworkId(Protos.FrameworkID.newBuilder().setValue("1234").build());
        IdentityManager identity = new IdentityManager(initial, stateStore);

        identity.register("test_id");

        DefaultConfigurationManager configurationManager =
                new DefaultConfigurationManager(CassandraSchedulerConfiguration.class,
                        config.createConfig().getServiceConfig().getName(),
                        server.getConnectString(),
                        config.createConfig(),
                        new ConfigValidator(),
                        stateStore);

        Capabilities mockCapabilities = Mockito.mock(Capabilities.class);
        when(mockCapabilities.supportsNamedVips()).thenReturn(true);
        ConfigurationManager configuration = new ConfigurationManager(
                new CassandraDaemonTask.Factory(mockCapabilities),
                configurationManager);

        cassandraState = new CassandraState(
                configuration,
                clusterTaskConfig,
                stateStore);

        taskFactory = new CassandraTaskFactory(executorDriver);
    }

    @After
    public void afterEach() throws Exception {
        server.close();
        server.stop();
    }

    @Test
    public void testCreateDaemonTask() throws ConfigStoreException, PersistenceException, ExecutorTaskException {
        CassandraDaemonTask daemonTask = cassandraState.createDaemon(testDaemonName);
        ExecutorTask executorTask = taskFactory.createTask(daemonTask.getTaskInfo(), executorDriver);
        Assert.assertNotNull(executorTask);

        ProcessTask processTask = (ProcessTask) executorTask;
        ProcessBuilder processBuilder = processTask.getProcessBuilder();
        Assert.assertNotNull(processBuilder);
        System.out.println("cmd: " + processBuilder.command());
    }
}
