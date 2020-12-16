package org.apache.kafka.controller;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.internals.QuotaConfigs;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.metadata.QuotaRecord;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.DescribeClientQuotasRequest;
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
import java.util.Optional;
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
     * @param quotaAlterations  List of client quota alterations to evalutate
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

    /**
     * Read the current client quotas from memory using the filters provided in the request
     *
     * @return
     */
    Map<ClientQuotaEntity, Map<String, Double>> describeClientQuotas(DescribeClientQuotasRequestData request) {
        verifyDescribeQuotaRequest(request);

        // entity type -> entity name match
        Map<String, Optional<String>> entityMatches = new HashMap<>(2);
        for (DescribeClientQuotasRequestData.ComponentData componentData : request.components()) {
            switch (componentData.matchType()) {
                case DescribeClientQuotasRequest.MATCH_TYPE_EXACT:
                    entityMatches.put(componentData.entityType(), Optional.of(componentData.match()));
                    break;
                case DescribeClientQuotasRequest.MATCH_TYPE_DEFAULT:
                    entityMatches.put(componentData.entityType(), Optional.of(QuotaConfigs.DEFAULT_ENTITY_NAME));
                    break;
                case DescribeClientQuotasRequest.MATCH_TYPE_SPECIFIED:
                    entityMatches.put(componentData.entityType(), Optional.empty());
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected match type: " + componentData.matchType());
            }
        }

        Set<Map.Entry<ClientQuotaEntity, Map<String, Double>>> allQuotas = clientQuotaData.entrySet();
        return allQuotas.stream()
            .filter(entry -> matchQuotaEntity(entry.getKey(), entityMatches, request.strict()))
            .collect(Collectors.toMap(entry -> desanitizeEntity(entry.getKey()), Map.Entry::getValue));
    }

    private void verifyDescribeQuotaRequest(DescribeClientQuotasRequestData request) {
        // Ensure we have only valid entity combinations and no duplicates
        List<String> entityTypesInRequest = request.components().stream()
                .map(DescribeClientQuotasRequestData.ComponentData::entityType)
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
                    throw new InvalidRequestException("Custom entity type" + entityType + " not supported");
            }
        });

        if (entityTypes.contains(ClientQuotaEntity.IP) && entityTypes.size() > 1) {
            throw new InvalidRequestException("Invalid entity filter component combination, IP filter component should " +
                    "not be used with user or clientId filter component.");
        }
    }

    private boolean matchQuotaEntity(ClientQuotaEntity targetEntity,
                                     Map<String, Optional<String>> entityMatches, boolean strict) {
        // Each entityMatch entry must match the target entity. In strict mode, we do not allow any unmatched
        // entries from the target entity
        Map<String, Boolean> matched = new HashMap<>(2);
        Set<String> unmatched = new HashSet<>(targetEntity.entries().keySet());
        for (Map.Entry<String, Optional<String>> searchEntry : entityMatches.entrySet()) {
            String searchType = searchEntry.getKey();
            Optional<String> searchName = searchEntry.getValue();

            String targetName = targetEntity.entries().get(searchType);
            if (targetName != null) {
                if (searchName.isPresent()) {
                    matched.put(searchType, searchName.get().equals(targetName));
                } else {
                    // empty search string means match anything
                    matched.put(searchType, true);
                }
                unmatched.remove(searchType);
            } else {
                matched.put(searchType, false);
            }

            /*
            String targetType = targetEntry.getKey();
            String targetName = targetEntry.getValue();
            Optional<String> searchName = entityMatches.get(targetType);
            if (searchName == null) {
                unmatched.add(targetType);
            } else {
                if (searchName.isPresent()) {
                    matched.put(targetType, searchName.get().equals(targetName));
                } else {
                    // empty match means match anything
                    matched.put(targetType, true);
                }
            }
             */
        }

        if (matched.containsValue(false)) {
            return false;
        } else if (strict && !unmatched.isEmpty()) {
            // No false matches, but we have unmatched entity types in strict mode
            return false;
        } else {
            return true;
        }
    }

    private void alterClientQuotaEntity(
            ClientQuotaEntity entity,
            Map<String, Double> newQuotaConfigs,
            List<ApiMessageAndVersion> outputRecords,
            Map<ClientQuotaEntity, ApiError> outputResults) {

        Map<String, String> sanitizedNames = new HashMap<>(3);
        ApiError error = sanitizeEntity(entity, sanitizedNames);
        if (error.isFailure()) {
            outputResults.put(entity, error);
            return;
        }

        Optional<String> user = Optional.ofNullable(sanitizedNames.get(ClientQuotaEntity.USER));
        Optional<String> clientId = Optional.ofNullable(sanitizedNames.get(ClientQuotaEntity.CLIENT_ID));
        Optional<String> ip = Optional.ofNullable(sanitizedNames.get(ClientQuotaEntity.IP));

        final Map<String, ConfigDef.ConfigKey> configKeys;
        if (user.isPresent() && clientId.isPresent() && !ip.isPresent()) {
            configKeys = QuotaConfigs.userConfigs().configKeys();
        } else if (user.isPresent() && !clientId.isPresent() && !ip.isPresent()) {
            configKeys = QuotaConfigs.userConfigs().configKeys();
        } else if (!user.isPresent() && clientId.isPresent() && !ip.isPresent()) {
            configKeys = QuotaConfigs.clientConfigs().configKeys();
        } else if (!user.isPresent() && !clientId.isPresent() && ip.isPresent()) {
            if (isValidIpEntity(ip.get())) {
                configKeys = QuotaConfigs.ipConfigs().configKeys();
            } else {
                outputResults.put(entity, new ApiError(Errors.INVALID_REQUEST, ip.get() + " is not a valid IP or resolvable host."));
                return;
            }
        } else {
            outputResults.put(entity, new ApiError(Errors.INVALID_REQUEST, "Invalid empty client quota entity"));
            return;
        }

        List<ApiMessageAndVersion> newRecords = new ArrayList<>();

        // Don't share objects with different records
        Supplier<List<QuotaRecord.EntityData>> recordEntitySupplier = () ->
            sanitizedNames.entrySet().stream().map(mapEntry -> new QuotaRecord.EntityData()
                .setEntityType(mapEntry.getKey())
                .setEntityName(mapEntry.getValue()))
            .collect(Collectors.toList());

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
                ApiError validationError = validateQuotaConfigKeyValue(configKeys.get(key), key, newValue);
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

    // TODO can this be shared with alter configs?
    private ApiError validateQuotaConfigKeyValue(ConfigDef.ConfigKey configKey, String key, Double value) {
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

    // TODO move this somewhere common?
    private boolean isValidIpEntity(String ip) {
        if (!ip.equals(QuotaConfigs.DEFAULT_ENTITY_NAME)) {
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

    private String sanitizeEntityName(String entityName) {
        if (entityName == null) {
            return QuotaConfigs.DEFAULT_ENTITY_NAME;
        } else {
            return Sanitizer.sanitize(entityName);
        }
    }

    private String desanitizeEntityName(String sanitizedEntityName) {
        if (sanitizedEntityName.equals(QuotaConfigs.DEFAULT_ENTITY_NAME)) {
            return null;
        } else {
            return Sanitizer.desanitize(sanitizedEntityName);
        }
    }

    private ClientQuotaEntity desanitizeEntity(ClientQuotaEntity entity) {
        Map<String, String> entries = new HashMap<>(2);
        entity.entries().forEach((type, name) -> entries.put(type, desanitizeEntityName(name)));
        return new ClientQuotaEntity(entries);
    }

    /**
     * Given a quota entity (which is a mapping of entity type to entity name), validate the types and
     * sanitize the names
     *
     * @param entity            The quota entity
     * @param sanitizedEntities A map to put the sanitized entity values into
     * @return                  An error if any validation failed
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

    /**
     * Apply a quota record to the in-memory state.
     *
     * @param record    A QuotaRecord instance.
     */
    public void replay(QuotaRecord record) {
        ClientQuotaEntity entity = new ClientQuotaEntity(
            record.entity().stream()
                .collect(Collectors.toMap(QuotaRecord.EntityData::entityType, QuotaRecord.EntityData::entityName)));
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
