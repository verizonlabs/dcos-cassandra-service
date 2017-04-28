package com.mesosphere.dcos.cassandra.scheduler.resources;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskRequest;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.util.*;
import java.util.stream.Collectors;

public class RepairRequest implements ClusterTaskRequest {

    public static final String ALL = "*";

    @JsonCreator
    public static RepairRequest create(
            @JsonProperty("nodes") final List<String> nodes,
            @JsonProperty("key_spaces") final List<String> keySpaces,
            @JsonProperty("column_families") final List<String>
                    columnFamilies) {
        return new RepairRequest(nodes, keySpaces, columnFamilies);
    }

    @JsonProperty("nodes")
    private final List<String> nodes;
    @JsonProperty("key_spaces")
    private final List<String> keySpaces;
    @JsonProperty("column_families")
    private final List<String> columnFamiles;

    private RepairRequest(
            final List<String> nodes,
            final List<String> keySpaces,
            final List<String> columnFamiles) {

        this.nodes = (nodes == null) ? Collections.emptyList() : nodes;
        this.keySpaces = (keySpaces == null) ? Collections.emptyList() :
                keySpaces;
        this.columnFamiles = (columnFamiles == null) ? Collections.emptyList() :
                columnFamiles;
    }


    private List<String> getColumnFamiles() {
        return columnFamiles;
    }

    private List<String> getKeySpaces() {
        return keySpaces;
    }

    private List<String> getNodes() {
        return nodes;
    }

    public boolean isValid() {
        return !nodes.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RepairRequest)) return false;
        RepairRequest that = (RepairRequest) o;
        return Objects.equals(getNodes(), that.getNodes()) &&
                Objects.equals(getKeySpaces(), that.getKeySpaces()) &&
                Objects.equals(getColumnFamiles(),
                        that.getColumnFamiles());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodes(), getKeySpaces(), getColumnFamiles());
    }

    @Override
    public String toString(){
        return JsonUtils.toJsonString(this);
    }

    public RepairContext toContext(CassandraState cassandraState) {
        return RepairContext.create(
                new ArrayList<>(getNodes(cassandraState)),
                getKeySpaces(),
                getColumnFamiles());
    }

    private Set<String> getNodes(CassandraState cassandraState) {
        final Set<String> allDaemons = cassandraState.getDaemons().keySet();
        if (getNodes().size() == 1 &&
                getNodes().get(0).equals(CleanupRequest.ALL)) {
            return allDaemons;
        } else {
            return getNodes().stream()
                    .filter(node -> allDaemons.contains(node))
                    .collect(Collectors.toSet());
        }
    }
}
