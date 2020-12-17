/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.internals.QuotaConfigs;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.metadata.QuotaRecord;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class ClientQuotaControlManager {
    private final SnapshotRegistry snapshotRegistry;

    final TimelineHashMap<ClientQuotaEntity, Map<String, Double>> clientQuotaData;

    ClientQuotaControlManager(SnapshotRegistry snapshotRegistry) {
        this.snapshotRegistry = snapshotRegistry;
        this.clientQuotaData = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * Determine the result of applying a batch of client quota alteration.  Note
     * that this method does not change the contents of memory.  It just generates a
     * result, that you can replay later if you wish using replay().
     *
     * @param quotaAlterations  List of client quota alterations to evaluate
     * @return                  The result.
     */
    ControllerResult<Map<ClientQuotaEntity, ApiError>> alterClientQuotas(
            Collection<ClientQuotaAlteration> quotaAlterations) {
        List<ApiMessageAndVersion> outputRecords = new ArrayList<>();
        Map<ClientQuotaEntity, ApiError> outputResults = new HashMap<>();

        quotaAlterations.forEach(quotaAlteration -> {
            // TODO check for duplicate keys?
            Map<String, Double> alterations = quotaAlteration.ops().stream()
                .collect(Collectors.toMap(ClientQuotaAlteration.Op::key, ClientQuotaAlteration.Op::value));
            alterClientQuotaEntity(quotaAlteration.entity(), alterations, outputRecords, outputResults);
        });

        return new ControllerResult<>(outputRecords, outputResults);
    }

    @FunctionalInterface
    interface EntityMatch {
        boolean matches(String name);
    }

    /**
     * Read the current client quotas from memory using the given quota filter
     *
     * @param filter    A ClientQuotaFilter built from the DescribeClientQuotasRequest
     * @return          Mapping of quota entity to the quota value map that match the given filter
     */
    Map<ClientQuotaEntity, Map<String, Double>> describeClientQuotas(ClientQuotaFilter filter) {
        verifyDescribeQuotaRequest(filter);

        // Use a slightly different convention from ClientQuotaFilter (no null optional)
        Map<String, EntityMatch> entityMatches = new HashMap<>(2);
        for (ClientQuotaFilterComponent filterComponent : filter.components()) {
            if (filterComponent.match() != null && filterComponent.match().isPresent()) {
                entityMatches.put(filterComponent.entityType(), name -> filterComponent.match().get().equals(name));
            } else if (filterComponent.match() != null) {
                entityMatches.put(filterComponent.entityType(), Objects::isNull);
            } else {
                entityMatches.put(filterComponent.entityType(), name -> true);
            }
        }

        // Traverse the in-memory quota entries and find matches
        Set<Map.Entry<ClientQuotaEntity, Map<String, Double>>> allQuotas = clientQuotaData.entrySet();
        return allQuotas.stream()
            .filter(entry -> matchQuotaEntity(entry.getKey(), entityMatches, filter.strict()))
            .filter(entry -> hasValidQuotaKeys(entry.getKey(), entry.getValue().keySet()))
            .collect(Collectors.toMap(entry -> desanitizeEntity(entry.getKey()), Map.Entry::getValue));
    }

    private void alterClientQuotaEntity(
            ClientQuotaEntity entity,
            Map<String, Double> newQuotaConfigs,
            List<ApiMessageAndVersion> outputRecords,
            Map<ClientQuotaEntity, ApiError> outputResults) {

        // Check entity types and sanitize the names
        Map<String, String> sanitizedNames = new HashMap<>(3);
        ApiError error = sanitizeEntity(entity, sanitizedNames);
        if (error.isFailure()) {
            outputResults.put(entity, error);
            return;
        }

        // Check the combination of entity types and get the config keys
        Map<String, ConfigDef.ConfigKey> configKeys = new HashMap<>(4);
        error = configKeysForEntityType(sanitizedNames, configKeys);
        if (error.isFailure()) {
            outputResults.put(entity, error);
            return;
        }

        // Don't share objects between different records
        Supplier<List<QuotaRecord.EntityData>> recordEntitySupplier = () ->
                sanitizedNames.entrySet().stream().map(mapEntry -> new QuotaRecord.EntityData()
                        .setEntityType(mapEntry.getKey())
                        .setEntityName(mapEntry.getValue()))
                        .collect(Collectors.toList());

        List<ApiMessageAndVersion> newRecords = new ArrayList<>(newQuotaConfigs.size());
        Map<String, Double> currentQuotas = clientQuotaData.getOrDefault(entity, Collections.emptyMap());
        newQuotaConfigs.forEach((key, newValue) -> {
            if (newValue == null) {
                if (currentQuotas.containsKey(key)) {
                    // Null value indicates removal
                    newRecords.add(new ApiMessageAndVersion(new QuotaRecord()
                            .setEntity(recordEntitySupplier.get())
                            .setKey(key)
                            .setRemove(true), (short) 0));
                }
            } else {
                ApiError validationError = validateQuotaConfigKeyValue(configKeys, key, newValue);
                if (validationError.isFailure()) {
                    outputResults.put(entity, validationError);
                } else {
                    final Double currentValue = currentQuotas.get(key);
                    if (!Objects.equals(currentValue, newValue)) {
                        // Only record the new value if it has changed
                        newRecords.add(new ApiMessageAndVersion(new QuotaRecord()
                                .setEntity(recordEntitySupplier.get())
                                .setKey(key)
                                .setValue(newValue), (short) 0));
                    }
                }
            }
        });

        outputRecords.addAll(newRecords);
        outputResults.put(entity, ApiError.NONE);
    }

    private ApiError configKeysForEntityType(Map<String, String> entity, Map<String, ConfigDef.ConfigKey> output) {
        // We only allow certain combinations of quota entity types. Which type is in use determines which config
        // keys are valid
        boolean hasUser = entity.containsKey(ClientQuotaEntity.USER);
        boolean hasClientId = entity.containsKey(ClientQuotaEntity.CLIENT_ID);
        boolean hasIp = entity.containsKey(ClientQuotaEntity.IP);

        final Map<String, ConfigDef.ConfigKey> configKeys;
        if (hasUser && hasClientId && !hasIp) {
            configKeys = QuotaConfigs.userConfigs().configKeys();
        } else if (hasUser && !hasClientId && !hasIp) {
            configKeys = QuotaConfigs.userConfigs().configKeys();
        } else if (!hasUser && hasClientId && !hasIp) {
            configKeys = QuotaConfigs.clientConfigs().configKeys();
        } else if (!hasUser && !hasClientId && hasIp) {
            if (isValidIpEntity(entity.get(ClientQuotaEntity.IP))) {
                configKeys = QuotaConfigs.ipConfigs().configKeys();
            } else {
                return new ApiError(Errors.INVALID_REQUEST, entity.get(ClientQuotaEntity.IP) + " is not a valid IP or resolvable host.");
            }
        } else {
            return new ApiError(Errors.INVALID_REQUEST, "Invalid empty client quota entity");
        }

        output.putAll(configKeys);
        return ApiError.NONE;
    }

    // TODO can this be shared with alter configs?
    private ApiError validateQuotaConfigKeyValue(Map<String, ConfigDef.ConfigKey> validKeys, String key, Double value) {
        ConfigDef.ConfigKey configKey = validKeys.get(key);
        if (configKey == null) {
            return new ApiError(Errors.INVALID_REQUEST, "Invalid configuration key " + key);
        }
        switch (configKey.type()) {
            case DOUBLE:
                break;
            case LONG:
                Double epsilon = 1e-6;
                Long longValue = Double.valueOf(value + epsilon).longValue();
                if (Math.abs(longValue.doubleValue() - value) > epsilon) {
                    return new ApiError(Errors.INVALID_REQUEST,
                            "Configuration " + key + " must be a Long value");
                }
                break;
            default:
                return new ApiError(Errors.UNKNOWN_SERVER_ERROR,
                        "Unexpected config type " + configKey.type() + " should be Long or Double");
        }
        return ApiError.NONE;
    }

    private void verifyDescribeQuotaRequest(ClientQuotaFilter filter) {
        // Ensure we have only valid entity combinations and no duplicates
        List<String> entityTypesInRequest = filter.components().stream()
                .map(ClientQuotaFilterComponent::entityType)
                .collect(Collectors.toList());

        Set<String> entityTypes = new HashSet<>(2);
        entityTypesInRequest.forEach(entityType -> {
            switch (entityType) {
                case ClientQuotaEntity.USER:
                    if (entityTypes.contains(ClientQuotaEntity.USER)) {
                        throw new InvalidRequestException("Duplicate user filter component entity type");
                    } else {
                        entityTypes.add(ClientQuotaEntity.USER);
                    }
                    break;
                case ClientQuotaEntity.CLIENT_ID:
                    if (entityTypes.contains(ClientQuotaEntity.CLIENT_ID)) {
                        throw new InvalidRequestException("Duplicate client id filter component entity type");
                    } else {
                        entityTypes.add(ClientQuotaEntity.CLIENT_ID);
                    }
                    break;
                case ClientQuotaEntity.IP:
                    if (entityTypes.contains(ClientQuotaEntity.IP)) {
                        throw new InvalidRequestException("Duplicate IP filter component entity type");
                    } else {
                        entityTypes.add(ClientQuotaEntity.IP);
                    }
                    break;
                case "":
                    throw new InvalidRequestException("Unexpected empty filter component entity type");
                default:
                    throw new InvalidRequestException("Custom entity type " + entityType + " not supported");
            }
        });

        if (entityTypes.contains(ClientQuotaEntity.IP) && entityTypes.size() > 1) {
            throw new InvalidRequestException("Invalid entity filter component combination, IP filter component should " +
                    "not be used with user or clientId filter component.");
        }
    }

    private boolean matchQuotaEntity(ClientQuotaEntity targetEntity,
                                     Map<String, EntityMatch> entityMatches, boolean strict) {
        // Each entityMatch entry must match the target entity. In strict mode, we do not allow any unmatched
        // entries from the target entity
        Map<String, Boolean> matched = new HashMap<>(2);
        Set<String> unmatched = new HashSet<>(targetEntity.entries().keySet());
        for (Map.Entry<String, EntityMatch> searchEntry : entityMatches.entrySet()) {
            String searchType = searchEntry.getKey();
            String targetName = targetEntity.entries().get(searchType);
            if (targetEntity.entries().containsKey(searchType)) {
                matched.put(searchType, searchEntry.getValue().matches(targetName));
                unmatched.remove(searchType);
            } else {
                matched.put(searchType, false);
            }
        }

        if (matched.containsValue(false)) {
            return false;
        } else {
            // If in strict mode, don't allow unmatched entities
            return !strict || unmatched.isEmpty();
        }
    }

    private boolean hasValidQuotaKeys(ClientQuotaEntity entity, Set<String> quotaKeys) {
        Map<String, ConfigDef.ConfigKey> configKeys = new HashMap<>();
        configKeysForEntityType(entity.entries(), configKeys);
        return quotaKeys.stream().allMatch(configKeys::containsKey);
    }

    // TODO move this somewhere common?
    private boolean isValidIpEntity(String ip) {
        if (Objects.nonNull(ip)) {
            try {
                InetAddress.getByName(ip);
                return true;
            } catch (UnknownHostException e) {
                return false;
            }
        } else {
            return true;
        }
    }

    /**
     * Given a quota entity (which is a mapping of entity type to entity name), validate the types and
     * sanitize the names
     */
    private ApiError sanitizeEntity(ClientQuotaEntity entity, Map<String, String> sanitizedEntities) {
        if (entity.entries().isEmpty()) {
            return new ApiError(Errors.INVALID_REQUEST, "Invalid empty client quota entity");
        }

        for (Map.Entry<String, String> entityEntry : entity.entries().entrySet()) {
            String entityType = entityEntry.getKey();
            String entityName = entityEntry.getValue();
            String sanitizedEntityName = sanitizeEntityName(entityName);
            if (Objects.equals(entityType, ClientQuotaEntity.USER)) {
                sanitizedEntities.put(ClientQuotaEntity.USER, sanitizedEntityName);
            } else if (Objects.equals(entityType, ClientQuotaEntity.CLIENT_ID)) {
                sanitizedEntities.put(ClientQuotaEntity.CLIENT_ID, sanitizedEntityName);
            } else if (Objects.equals(entityType, ClientQuotaEntity.IP)) {
                sanitizedEntities.put(ClientQuotaEntity.IP, sanitizedEntityName);
            } else {
                return new ApiError(Errors.INVALID_REQUEST, "Unhandled client quota entity type: " + entityType);
            }

            if (entityName != null && entityName.isEmpty()) {
                return new ApiError(Errors.INVALID_REQUEST, "Empty " + entityType + " not supported");
            }
        }

        return ApiError.NONE;
    }

    private String sanitizeEntityName(String entityName) {
        if (entityName == null) {
            return null;
        } else {
            return Sanitizer.sanitize(entityName);
        }
    }

    private ClientQuotaEntity desanitizeEntity(ClientQuotaEntity entity) {
        Map<String, String> entries = new HashMap<>(2);
        entity.entries().forEach((type, name) -> entries.put(type, desanitizeEntityName(name)));
        return new ClientQuotaEntity(entries);
    }

    private String desanitizeEntityName(String sanitizedEntityName) {
        if (sanitizedEntityName == null) {
            return null;
        } else {
            return Sanitizer.desanitize(sanitizedEntityName);
        }
    }

    /**
     * Apply a quota record to the in-memory state.
     *
     * @param record    A QuotaRecord instance.
     */
    public void replay(QuotaRecord record) {
        Map<String, String> entityMap = new HashMap<>(2);
        record.entity().forEach(entityData -> entityMap.put(entityData.entityType(), entityData.entityName()));
        ClientQuotaEntity entity = new ClientQuotaEntity(entityMap);
        Map<String, Double> quotas = clientQuotaData.get(entity);
        if (quotas == null) {
            quotas = new TimelineHashMap<>(snapshotRegistry, 0);
            clientQuotaData.put(entity, quotas);
        }
        if (record.remove()) {
            quotas.remove(record.key());
        } else {
            quotas.put(record.key(), record.value());
        }
    }
}
