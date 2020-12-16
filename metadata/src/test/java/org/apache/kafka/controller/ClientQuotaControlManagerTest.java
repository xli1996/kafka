package org.apache.kafka.controller;

import org.apache.kafka.common.config.internals.QuotaConfigs;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.metadata.QuotaRecord;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.DescribeClientQuotasRequest.MATCH_TYPE_DEFAULT;
import static org.apache.kafka.common.requests.DescribeClientQuotasRequest.MATCH_TYPE_EXACT;
import static org.apache.kafka.common.requests.DescribeClientQuotasRequest.MATCH_TYPE_SPECIFIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(value = 40)
public class ClientQuotaControlManagerTest {
    @Test
    public void testDescribeClientQuotas() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(0);
        ClientQuotaControlManager manager = new ClientQuotaControlManager(snapshotRegistry);

        manager.clientQuotaData.put(userEntity("A"), quotas("foo", 1.0, "bar", 2.0));
        manager.clientQuotaData.put(userClientEntity("A", "X"), quotas("spam", 1.0));
        manager.clientQuotaData.put(userClientEntity("A", "Y"), quotas("eggs", 1.0));

        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData();
        request.setStrict(false);
        request.setComponents(Collections.singletonList(
            new DescribeClientQuotasRequestData.ComponentData()
                .setEntityType(ClientQuotaEntity.USER).setMatch("A").setMatchType(MATCH_TYPE_EXACT)));
        Map<ClientQuotaEntity, Map<String, Double>> quotas = manager.describeClientQuotas(request);
        assertEquals(3, quotas.size());

        request.setStrict(true);
        quotas = manager.describeClientQuotas(request);
        assertEquals(1, quotas.size());
    }

    @Test
    public void testDesanitizedNames() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(0);
        ClientQuotaControlManager manager = new ClientQuotaControlManager(snapshotRegistry);

        String name = "name with *3* spaces";
        manager.clientQuotaData.put(userEntity(Sanitizer.sanitize(name)),
            quotas("foo", 1.0, "bar", 2.0));
        manager.clientQuotaData.put(userClientEntity(Sanitizer.sanitize(name), "X"), quotas("spam", 1.0));
        manager.clientQuotaData.put(userClientEntity(Sanitizer.sanitize(name), "Y"), quotas("eggs", 1.0));

        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData();
        request.setStrict(false);
        request.setComponents(Collections.singletonList(
                new DescribeClientQuotasRequestData.ComponentData()
                        .setEntityType(ClientQuotaEntity.USER).setMatchType(MATCH_TYPE_SPECIFIED)));
        Map<ClientQuotaEntity, Map<String, Double>> quotas = manager.describeClientQuotas(request);

        assertEquals(3, quotas.size());
        assertTrue(quotas.keySet().stream()
            .allMatch(quotaEntity -> quotaEntity.entries().get(ClientQuotaEntity.USER).equals(name)));
    }

    @Test
    public void testInvalidDescribeFilters() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(0);
        ClientQuotaControlManager manager = new ClientQuotaControlManager(snapshotRegistry);
        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData();
        request.setStrict(false);
        request.setComponents(Arrays.asList(
                new DescribeClientQuotasRequestData.ComponentData()
                        .setEntityType(ClientQuotaEntity.USER).setMatchType(MATCH_TYPE_SPECIFIED),
                new DescribeClientQuotasRequestData.ComponentData()
                        .setEntityType(ClientQuotaEntity.IP).setMatchType(MATCH_TYPE_SPECIFIED)));

        try {
            manager.describeClientQuotas(request);
            throw new AssertionError("Expected an error");
        } catch (InvalidRequestException e) {
            assertTrue(e.getMessage().startsWith("Invalid entity filter component combination"));
        } catch (Throwable t) {
            fail("Should not see this error", t);
        }
    }

    public void setupDescribeMatchTest(ClientQuotaControlManager manager,
                                       BiConsumer<ClientQuotaEntity, Map<String, Double>> verifier) {

        Map<ClientQuotaEntity, Map<String, Double>> quotasToTest = new HashMap<>();
        quotasToTest.put(userClientEntity("user-1", "client-id-1"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 50.50));
        quotasToTest.put(userClientEntity("user-2", "client-id-1"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 51.51));
        quotasToTest.put(userClientEntity("user-3", "client-id-2"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 52.52));
        quotasToTest.put(userClientEntity(null, "client-id-1"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 53.53));
        quotasToTest.put(userClientEntity("user-1", null),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 54.54));
        quotasToTest.put(userClientEntity("user-3", null),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 55.55));
        quotasToTest.put(userEntity("user-1"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 56.56));
        quotasToTest.put(userEntity("user-2"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 57.57));
        quotasToTest.put(userEntity("user-3"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 58.58));
        quotasToTest.put(userEntity(null),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 59.59));
        quotasToTest.put(clientEntity("client-id-2"),
                quotas(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 60.60));


        List<ClientQuotaAlteration> alters = new ArrayList<>();
        quotasToTest.forEach((entity, quota) -> {
            Collection<ClientQuotaAlteration.Op> ops = quota.entrySet().stream()
                    .map(quotaEntry -> new ClientQuotaAlteration.Op(quotaEntry.getKey(), quotaEntry.getValue()))
                    .collect(Collectors.toList());
            alters.add(new ClientQuotaAlteration(entity, ops));
        });
        ControllerResult<?> result = manager.alterClientQuotas(alters);
        result.records().forEach(apiMessageAndVersion -> manager.replay((QuotaRecord) apiMessageAndVersion.message()));
        quotasToTest.forEach(verifier);
    }

    @Test
    public void testDescribeMatchExact() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(0);
        ClientQuotaControlManager manager = new ClientQuotaControlManager(snapshotRegistry);

        setupDescribeMatchTest(manager, (entity, quotas) -> {
            DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData();
            request.setStrict(true);
            request.setComponents(new ArrayList<>());
            entityToRequest(entity, request.components()::add);

            // Exact match should only return one result for each in our test set
            assertEquals(1, manager.describeClientQuotas(request).size());
        });

        List<ClientQuotaEntity> nonMatching = Arrays.asList(
            userClientEntity("user-1", "client-id-2"),
            userClientEntity("user-3", "client-id-1"),
            userClientEntity("user-2", null),
            userEntity("user-4"),
            userClientEntity(null, "client-id-2"),
            clientEntity("client-id-1"),
            clientEntity("client-id-3")
        );

        nonMatching.forEach(entity -> {
            DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData();
            request.setStrict(true);
            request.setComponents(new ArrayList<>());
            entityToRequest(entity, request.components()::add);

            assertEquals(0, manager.describeClientQuotas(request).size());
        });
    }

    @Test
    public void testDescribeMatchPartial() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(0);
        ClientQuotaControlManager manager = new ClientQuotaControlManager(snapshotRegistry);

        setupDescribeMatchTest(manager, (entity, quotas) -> {});

        DescribeClientQuotasRequestData request = new DescribeClientQuotasRequestData();
        request.setStrict(false);
        request.setComponents(new ArrayList<>());

        // Match open-ended existing user.
        entityToRequest(userEntity("user-1"), request.components()::add);
        Map<ClientQuotaEntity, Map<String, Double>> matched = manager.describeClientQuotas(request);
        assertEquals(3, matched.size());
        assertTrue(matched.keySet().stream()
            .allMatch(entity -> entity.entries().get(ClientQuotaEntity.USER).equals("user-1")));

        // Match open-ended non-existent user.
        request.components().clear();
        entityToRequest(userEntity("unknown"), request.components()::add);
        assertEquals(0, manager.describeClientQuotas(request).size());

        // Match open-ended existing client ID.
        request.components().clear();
        entityToRequest(clientEntity("client-id-2"), request.components()::add);
        matched = manager.describeClientQuotas(request);
        assertEquals(2, matched.size());
        assertTrue(matched.keySet().stream()
                .allMatch(entity -> entity.entries().get(ClientQuotaEntity.CLIENT_ID).equals("client-id-2")));


        // TODO more cases
    }

    static void entityToRequest(ClientQuotaEntity entity, Consumer<DescribeClientQuotasRequestData.ComponentData> acceptor) {
        entity.entries().forEach((type, name) -> {
            if (name == null) {
                acceptor.accept(new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(type).setMatchType(MATCH_TYPE_DEFAULT));
            } else {
                acceptor.accept(new DescribeClientQuotasRequestData.ComponentData()
                    .setEntityType(type).setMatch(name).setMatchType(MATCH_TYPE_EXACT));
            }
        });
    }

    static Map<String, Double> quotas(String key, Double value) {
        return Collections.singletonMap(key, value);
    }

    static Map<String, Double> quotas(String key1, Double value1, String key2, Double value2) {
        Map<String, Double> quotas = new HashMap<>(2);
        quotas.put(key1, value1);
        quotas.put(key2, value2);
        return quotas;
    }

    static ClientQuotaEntity userEntity(String user) {
        return new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, user));
    }

    static ClientQuotaEntity clientEntity(String clientId) {
        return new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.CLIENT_ID, clientId));
    }

    static ClientQuotaEntity userClientEntity(String user, String clientId) {
        Map<String, String> entries = new HashMap<>(2);
        entries.put(ClientQuotaEntity.USER, user);
        entries.put(ClientQuotaEntity.CLIENT_ID, clientId);
        return new ClientQuotaEntity(entries);
    }
}
