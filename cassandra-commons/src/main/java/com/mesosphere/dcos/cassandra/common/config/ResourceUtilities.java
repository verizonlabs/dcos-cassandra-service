package com.mesosphere.dcos.cassandra.common.config;

/**
 * Created by hansti2 on 10/19/16.
 */

import org.apache.mesos.Protos;
import org.apache.mesos.offer.MesosResource;
import org.apache.mesos.offer.ValueUtils;
import org.apache.mesos.specification.ResourceSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;package org.apache.mesos.offer;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.Resource.DiskInfo;
import org.apache.mesos.Protos.Resource.DiskInfo.Persistence;
import org.apache.mesos.Protos.Resource.DiskInfo.Source;
import org.apache.mesos.Protos.Resource.ReservationInfo;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.specification.ResourceSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This class encapsulates common methods for manipulating Resources.
 */
public class ResourceUtilities {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceUtilities.class);

    public static Protos.Resource getUnreservedResource(String name, Protos.Value value) {
        return setResource(Protos.Resource.newBuilder().setRole("*"), name, value);
    }

    public static Protos.Resource getDesiredResource(ResourceSpecification resourceSpecification) {
        return getDesiredResource(
                resourceSpecification.getRole(),
                resourceSpecification.getPrincipal(),
                resourceSpecification.getName(),
                resourceSpecification.getValue());
    }

    public static Protos.Resource getUnreservedMountVolume(double diskSize, String mountRoot) {
        Protos.Value diskValue = Protos.Value.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(diskSize))
                .build();
        Protos.Resource.Builder resBuilder = Protos.Resource.newBuilder(org.apache.mesos.offer.ResourceUtils.getUnreservedResource("disk", diskValue));
        resBuilder.setRole("*");
        resBuilder.setDisk(getUnreservedMountVolumeDiskInfo(mountRoot));

        return resBuilder.build();
    }

    public static Protos.Resource getDesiredMountVolume(String role, String principal, double diskSize, String containerPath) {
        Protos.Value diskValue = Protos.Value.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(diskSize))
                .build();
        Protos.Resource.Builder resBuilder = Protos.Resource.newBuilder(getUnreservedResource("disk", diskValue));
        resBuilder.setRole(role);
        resBuilder.setReservation(getDesiredReservationInfo(principal));
        resBuilder.setDisk(getDesiredMountVolumeDiskInfo(principal, containerPath));
        return resBuilder.build();
    }


    public static Protos.Resource getDesiredMountVolume(String role, String principal, double diskSize, String containerPath, String rootPath) {
        Protos.Value diskValue = Protos.Value.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(diskSize))
                .build();
        Protos.Resource.Builder resBuilder = Protos.Resource.newBuilder(getUnreservedResource("disk", diskValue));
        resBuilder.setRole(role);
        resBuilder.setReservation(getDesiredReservationInfo(principal));
        resBuilder.setDisk(getDesiredMountVolumeDiskInfo(principal, containerPath, rootPath));
        return resBuilder.build();
    }


    public static Protos.Resource getExpectedMountVolume(
            double diskSize,
            String resourceId,
            String role,
            String principal,
            String mountRoot,
            String containerPath,
            String persistenceId) {
        Protos.Value diskValue = Protos.Value.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(diskSize))
                .build();
        Protos.Resource.Builder resBuilder = Protos.Resource.newBuilder(org.apache.mesos.offer.ResourceUtils.getUnreservedResource("disk", diskValue));
        resBuilder.setRole(role);
        resBuilder.setDisk(getExpectedMountVolumeDiskInfo(mountRoot, containerPath, persistenceId, principal));
        resBuilder.setReservation(getExpectedReservationInfo(resourceId, principal));

        return resBuilder.build();
    }

    public static Protos.Resource getUnreservedRootVolume(double diskSize) {
        Protos.Value diskValue = Protos.Value.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(diskSize))
                .build();
        Protos.Resource.Builder resBuilder = Protos.Resource.newBuilder(org.apache.mesos.offer.ResourceUtils.getUnreservedResource("disk", diskValue));
        resBuilder.setRole("*");
        return resBuilder.build();
    }

    public static Protos.Resource getDesiredRootVolume(String role, String principal, double diskSize, String containerPath) {
        Protos.Value diskValue = Protos.Value.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(diskSize))
                .build();
        Protos.Resource.Builder resBuilder = Protos.Resource.newBuilder(getUnreservedResource("disk", diskValue));
        resBuilder.setRole(role);
        resBuilder.setReservation(getDesiredReservationInfo(principal));
        resBuilder.setDisk(getDesiredRootVolumeDiskInfo(principal, containerPath));
        return resBuilder.build();
    }

    public static Protos.Resource getDesiredRootVolume(String role, String principal, double diskSize, String containerPath, String rootPath) {
        Protos.Value diskValue = Protos.Value.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(diskSize))
                .build();
        Protos.Resource.Builder resBuilder = Protos.Resource.newBuilder(getUnreservedResource("disk", diskValue));
        resBuilder.setRole(role);
        resBuilder.setReservation(getDesiredReservationInfo(principal));
        resBuilder.setDisk(getDesiredRootVolumeDiskInfo(principal, containerPath, rootPath));
        return resBuilder.build();
    }

    public static Protos.Resource getExpectedRootVolume(
            double diskSize,
            String resourceId,
            String role,
            String principal,
            String persistenceId) {
        Protos.Value diskValue = Protos.Value.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(diskSize))
                .build();
        Protos.Resource.Builder resBuilder = Protos.Resource.newBuilder(org.apache.mesos.offer.ResourceUtils.getUnreservedResource("disk", diskValue));
        resBuilder.setRole(role);
        resBuilder.setDisk(getExpectedRootVolumeDiskInfo(persistenceId, principal));
        resBuilder.setReservation(getExpectedReservationInfo(resourceId, principal));

        return resBuilder.build();
    }

    public static Protos.Resource getDesiredResource(String role, String principal, String name, Protos.Value value) {
        return Protos.Resource.newBuilder(getUnreservedResource(name, value))
                .setRole(role)
                .setReservation(getDesiredReservationInfo(principal))
                .build();
    }

    public static Protos.Resource getUnreservedScalar(String name, double value) {
        Protos.Value val = Protos.Value.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(value))
                .build();
        Protos.Resource.Builder resBuilder = Protos.Resource.newBuilder(org.apache.mesos.offer.ResourceUtils.getUnreservedResource(name, val));
        resBuilder.setRole("*");

        return resBuilder.build();
    }

    public static Protos.Resource getExpectedScalar(
            String name,
            double value,
            String resourceId,
            String role,
            String principal) {
        Protos.Value val = Protos.Value.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(value))
                .build();
        Protos.Resource.Builder resBuilder = Protos.Resource.newBuilder(org.apache.mesos.offer.ResourceUtils.getUnreservedResource(name, val));
        resBuilder.setRole(role);
        resBuilder.setReservation(getExpectedReservationInfo(resourceId, principal));

        return resBuilder.build();
    }

    public static Protos.Resource getDesiredScalar(String role, String principal, String name, double value) {
        Protos.Value val = Protos.Value.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(value))
                .build();
        return getDesiredResource(role, principal, name, val);
    }

    public static Protos.Resource getUnreservedRanges(String name, List<Protos.Value.Range> ranges) {
        Protos.Value val = Protos.Value.newBuilder()
                .setType(Protos.Value.Type.RANGES)
                .setRanges(Protos.Value.Ranges.newBuilder().addAllRange(ranges))
                .build();
        Protos.Resource.Builder resBuilder = Protos.Resource.newBuilder(org.apache.mesos.offer.ResourceUtils.getUnreservedResource(name, val));
        resBuilder.setRole("*");

        return resBuilder.build();
    }

    public static Protos.Resource getDesiredRanges(String role, String principal, String name, List<Protos.Value.Range> ranges) {
        return getDesiredResource(
                role,
                principal,
                name,
                Protos.Value.newBuilder()
                        .setType(Protos.Value.Type.RANGES)
                        .setRanges(Protos.Value.Ranges.newBuilder()
                                .addAllRange(ranges)
                                .build())
                        .build());
    }

    public static Protos.Resource getExpectedRanges(
            String name,
            List<Protos.Value.Range> ranges,
            String resourceId,
            String role,
            String principal) {

        Protos.Value val = Protos.Value.newBuilder()
                .setType(Protos.Value.Type.RANGES)
                .setRanges(Protos.Value.Ranges.newBuilder().addAllRange(ranges))
                .build();
        Protos.Resource.Builder resBuilder = Protos.Resource.newBuilder(org.apache.mesos.offer.ResourceUtils.getUnreservedResource(name, val));
        resBuilder.setRole(role);
        resBuilder.setReservation(getExpectedReservationInfo(resourceId, principal));

        return resBuilder.build();
    }

    public static Protos.Resource setValue(Protos.Resource resource, Protos.Value value) {
        return setResource(Protos.Resource.newBuilder(resource), resource.getName(), value);
    }

    public static Protos.Resource setResourceId(Protos.Resource resource, String resourceId) {
        return Protos.Resource.newBuilder(resource)
                .setReservation(setResourceId(resource.getReservation(), resourceId))
                .build();
    }

    public static String getResourceId(Protos.Resource resource) {
        if (resource.hasReservation() && resource.getReservation().hasLabels()) {
            for (Protos.Label label : resource.getReservation().getLabels().getLabelsList()) {
                if (label.getKey().equals(MesosResource.RESOURCE_ID_KEY)) {
                    return label.getValue();
                }
            }
        }
        return null;
    }

    public static String getPersistenceId(Protos.Resource resource) {
        if (resource.hasDisk() && resource.getDisk().hasPersistence()) {
            return resource.getDisk().getPersistence().getId();
        }

        return null;
    }

    public static Protos.TaskInfo clearResourceIds(Protos.TaskInfo taskInfo) {
        List<Protos.Resource> clearedTaskResources = clearResourceIds(taskInfo.getResourcesList());
        Protos.TaskInfo.Builder taskInfoBuilder = Protos.TaskInfo.newBuilder(taskInfo)
                .clearResources()
                .addAllResources(clearedTaskResources);

        if (taskInfo.hasExecutor()) {
            taskInfoBuilder.setExecutor(clearResourceIds(taskInfo.getExecutor()));
        }

        return taskInfoBuilder.build();
    }

    public static Protos.ExecutorInfo clearResourceIds(Protos.ExecutorInfo executorInfo) {
        List<Protos.Resource> clearedResources = clearResourceIds(executorInfo.getResourcesList());
        return Protos.ExecutorInfo.newBuilder(executorInfo)
                .clearResources()
                .addAllResources(clearedResources)
                .build();
    }

    public static boolean areDifferent(
            ResourceSpecification oldResourceSpecification,
            ResourceSpecification newResourceSpecification) {

        Protos.Value oldValue = oldResourceSpecification.getValue();
        Protos.Value newValue = newResourceSpecification.getValue();
        if (!ValueUtils.equal(oldValue, newValue)) {
            LOGGER.info(String.format("Values '%s' and '%s' are different.", oldValue, newValue));
            return true;
        }

        String oldRole = oldResourceSpecification.getRole();
        String newRole = newResourceSpecification.getRole();
        if (!Objects.equals(oldRole, newRole)) {
            LOGGER.info(String.format("Roles '%s' and '%s' are different.", oldRole, newRole));
            return true;
        }

        String oldPrincipal = oldResourceSpecification.getPrincipal();
        String newPrincipal = newResourceSpecification.getPrincipal();
        if (!Objects.equals(oldPrincipal, newPrincipal)) {
            LOGGER.info(String.format("Principals '%s' and '%s' are different.", oldPrincipal, newPrincipal));
            return true;
        }

        return false;
    }

    public static Protos.Resource updateResource(Protos.Resource resource, ResourceSpecification resourceSpecification)
            throws IllegalArgumentException {
        Protos.Resource.Builder builder = Protos.Resource.newBuilder(resource);
        switch (resource.getType()) {
            case SCALAR:
                return builder.setScalar(resourceSpecification.getValue().getScalar()).build();
            case RANGES:
                return builder.setRanges(resourceSpecification.getValue().getRanges()).build();
            case SET:
                return builder.setSet(resourceSpecification.getValue().getSet()).build();
            default:
                throw new IllegalArgumentException("Unexpected Resource type encountered: " + resource.getType());
        }
    }

    private static List<Protos.Resource> clearResourceIds(List<Protos.Resource> resources) {
        List<Protos.Resource> clearedResources = new ArrayList<>();

        for (Protos.Resource resource : resources) {
            clearedResources.add(clearResourceId(resource));
        }

        return clearedResources;
    }

    private static Protos.Resource clearResourceId(Protos.Resource resource) {
        if (resource.hasReservation()) {
            List<Protos.Label> labels = resource.getReservation().getLabels().getLabelsList();

            Protos.Resource.Builder resourceBuilder = Protos.Resource.newBuilder(resource);
            Protos.Resource.ReservationInfo.Builder reservationBuilder = Protos.Resource.ReservationInfo
                    .newBuilder(resource.getReservation());

            Protos.Labels.Builder labelsBuilder = Protos.Labels.newBuilder();
            for (Protos.Label label : labels) {
                if (!label.getKey().equals(MesosResource.RESOURCE_ID_KEY)) {
                    labelsBuilder.addLabels(label);
                }
            }

            reservationBuilder.setLabels(labelsBuilder.build());
            resourceBuilder.setReservation(reservationBuilder.build());
            return resourceBuilder.build();
        } else {
            return resource;
        }
    }

    private static Protos.Resource setResource(Protos.Resource.Builder resBuilder, String name, Protos.Value value) {
        Protos.Value.Type type = value.getType();

        resBuilder
                .setName(name)
                .setType(type);

        switch (type) {
            case SCALAR:
                return resBuilder.setScalar(value.getScalar()).build();
            case RANGES:
                return resBuilder.setRanges(value.getRanges()).build();
            case SET:
                return resBuilder.setSet(value.getSet()).build();
            default:
                return null;
        }
    }

    private static Protos.Resource.ReservationInfo setResourceId(Protos.Resource.ReservationInfo resInfo, String resourceId) {
        return Protos.Resource.ReservationInfo.newBuilder(resInfo)
                .setLabels(setResourceId(resInfo.getLabels(), resourceId))
                .build();
    }

    public static Protos.Labels setResourceId(Protos.Labels labels, String resourceId) {
        Protos.Labels.Builder labelsBuilder = Protos.Labels.newBuilder();

        // Copy everything except blank resource ID label
        for (Protos.Label label : labels.getLabelsList()) {
            String key = label.getKey();
            String value = label.getValue();
            if (!key.equals(MesosResource.RESOURCE_ID_KEY)) {
                labelsBuilder.addLabels(Protos.Label.newBuilder()
                        .setKey(key)
                        .setValue(value)
                        .build());
            }
        }

        labelsBuilder.addLabels(Protos.Label.newBuilder()
                .setKey(MesosResource.RESOURCE_ID_KEY)
                .setValue(resourceId)
                .build());

        return labelsBuilder.build();
    }

    private static Protos.Resource.ReservationInfo getDesiredReservationInfo(String principal) {
        return getDesiredReservationInfo(principal, "");
    }

    private static Protos.Resource.ReservationInfo getDesiredReservationInfo(String principal, String reservationId) {
        return Protos.Resource.ReservationInfo.newBuilder()
                .setPrincipal(principal)
                .setLabels(getDesiredReservationLabels(reservationId))
                .build();
    }

    private static Protos.Resource.ReservationInfo getExpectedReservationInfo(String resourceId, String principal) {
        return Protos.Resource.ReservationInfo.newBuilder()
                .setPrincipal(principal)
                .setLabels(Protos.Labels.newBuilder()
                        .addLabels(Protos.Label.newBuilder()
                                .setKey(MesosResource.RESOURCE_ID_KEY)
                                .setValue(resourceId)
                                .build())
                        .build())
                .build();
    }

    private static Protos.Labels getDesiredReservationLabels(String resourceId) {
        return Protos.Labels.newBuilder()
                .addLabels(
                        Protos.Label.newBuilder()
                                .setKey(MesosResource.RESOURCE_ID_KEY)
                                .setValue(resourceId)
                                .build())
                .build();
    }

    private static Protos.Resource.DiskInfo getUnreservedMountVolumeDiskInfo(String mountRoot) {
        return Protos.Resource.DiskInfo.newBuilder()
                .setSource(Protos.Resource.DiskInfo.Source.newBuilder()
                        .setType(Protos.Resource.DiskInfo.Source.Type.MOUNT)
                        .setMount(Protos.Resource.DiskInfo.Source.Mount.newBuilder()
                                .setRoot(mountRoot)
                                .build())
                        .build())
                .build();
    }

    private static Protos.Resource.DiskInfo getDesiredMountVolumeDiskInfo(String principal, String containerPath) {
        return Protos.Resource.DiskInfo.newBuilder()
                .setPersistence(Protos.Resource.DiskInfo.Persistence.newBuilder()
                        .setId("")
                        .setPrincipal(principal)
                        .build())
                .setSource(getDesiredMountVolumeSource())
                .setVolume(Protos.Volume.newBuilder()
                        .setContainerPath(containerPath)
                        .setMode(Protos.Volume.Mode.RW)
                        .build())
                .build();
    }

    private static Protos.Resource.DiskInfo getDesiredMountVolumeDiskInfo(String principal, String containerPath, String rootPath) {
        return Protos.Resource.DiskInfo.newBuilder()
                .setPersistence(Protos.Resource.DiskInfo.Persistence.newBuilder()
                        .setId("")
                        .setPrincipal(principal)
                        .build())
                .setVolume(Protos.Volume.newBuilder()
                        .setContainerPath(containerPath)
                        .setHostPath(rootPath)
                        .setMode(Protos.Volume.Mode.RW)
                        .build())
                .build();
    }

    private static DiskInfo.Source createSource(String source){
        return Protos.Resource.DiskInfo.Source.newBuilder()
                .setPath(Source.Path.newBuilder().setRoot(source))
                .build();
    }
    private static Protos.Resource.DiskInfo getExpectedMountVolumeDiskInfo(
            String mountRoot,
            String containerPath,
            String persistenceId,
            String principal) {
        return Protos.Resource.DiskInfo.newBuilder(getUnreservedMountVolumeDiskInfo(mountRoot))
                .setPersistence(Protos.Resource.DiskInfo.Persistence.newBuilder()
                        .setId(persistenceId)
                        .setPrincipal(principal)
                        .build())
                .setVolume(Protos.Volume.newBuilder()
                        .setContainerPath(containerPath)
                        .setMode(Protos.Volume.Mode.RW)
                        .build())
                .build();
    }

    private static Protos.Resource.DiskInfo getDesiredRootVolumeDiskInfo(String principal, String containerPath) {
        return Protos.Resource.DiskInfo.newBuilder()
                .setPersistence(Protos.Resource.DiskInfo.Persistence.newBuilder()
                        .setId("")
                        .setPrincipal(principal)
                        .build())
                .setVolume(Protos.Volume.newBuilder()
                        .setContainerPath(containerPath)
                        .setMode(Protos.Volume.Mode.RW)
                        .build())
                .build();
    }

    private static Protos.Resource.DiskInfo getDesiredRootVolumeDiskInfo(String principal, String containerPath, String rootPath) {
        return Protos.Resource.DiskInfo.newBuilder()
                .setPersistence(Protos.Resource.DiskInfo.Persistence.newBuilder()
                        .setId("")
                        .setPrincipal(principal)
                        .build())
                .setVolume(Protos.Volume.newBuilder()
                        .setContainerPath(containerPath)
                        .setHostPath(rootPath)
                        .setMode(Protos.Volume.Mode.RW)
                        .build())
                .build();
    }


    private static Protos.Resource.DiskInfo getExpectedRootVolumeDiskInfo(String persistenceId, String principal) {
        return Protos.Resource.DiskInfo.newBuilder()
                .setPersistence(Protos.Resource.DiskInfo.Persistence.newBuilder()
                        .setId(persistenceId)
                        .setPrincipal(principal)
                        .build())
                .build();
    }

    private static Protos.Resource.DiskInfo.Source getDesiredMountVolumeSource() {
        return Protos.Resource.DiskInfo.Source.newBuilder().setType(Protos.Resource.DiskInfo.Source.Type.MOUNT).build();
    }
}
