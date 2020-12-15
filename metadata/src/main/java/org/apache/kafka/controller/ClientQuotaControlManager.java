package org.apache.kafka.controller;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.internals.QuotaConfigs;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.metadata.QuotaRecord;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ClientQuotaControlManager {
    private final SnapshotRegistry snapshotRegistry;
    private final Map<String, ConfigDef> configDefs;
    private final TimelineHashMap<ClientQuotaEntity, TimelineHashMap<String, Double>> clientQuotaData;

    ClientQuotaControlManager(SnapshotRegistry snapshotRegistry, Map<String, ConfigDef> configDefs) {
        this.snapshotRegistry = snapshotRegistry;
        this.configDefs = configDefs;
        this.clientQuotaData = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     *
     * @param quotaAlterations
     * @return
     */
    ControllerResult<Map<ClientQuotaEntity, ApiError>> alterClientQuotas(
            List<ClientQuotaAlteration> quotaAlterations) {
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

    private void alterClientQuotaEntity(
            ClientQuotaEntity entity,
            Map<String, Double> newQuotaConfigs,
            List<ApiMessageAndVersion> outputRecords,
            Map<ClientQuotaEntity, ApiError> outputResults) {
        final Map<String, ConfigDef.ConfigKey> configKeys;

        Map<String, String> sanitizedNames = new HashMap<>(2);
        ApiError error = entityToSanitizedUserClientId(entity, sanitizedNames);
        if (error.isFailure()) {
            outputResults.put(entity, error);
            return;
        }

        Optional<String> user = Optional.ofNullable(sanitizedNames.get(ClientQuotaEntity.USER));
        Optional<String> clientId = Optional.ofNullable(sanitizedNames.get(ClientQuotaEntity.CLIENT_ID));
        if (user.isPresent()) {
            // TODO get this string from somewhere. AdminManager got these ConfigDefs from DynamicConfig.User and DynamicConfig.Client
            configKeys = configDefs.get("user").configKeys();
        } else if (clientId.isPresent()) {
            configKeys = configDefs.get("client").configKeys();
        } else {
            outputResults.put(entity, new ApiError(Errors.INVALID_REQUEST, "Invalid empty client quota entity"));
            throw new RuntimeException();
        }

        List<ApiMessageAndVersion> newRecords = new ArrayList<>();

        // Don't share objects with different records
        Supplier<List<QuotaRecord.EntityData>> recordEntitySupplier = () ->
            entity.entries().entrySet().stream().map(mapEntry -> new QuotaRecord.EntityData()
                .setEntityType(mapEntry.getKey())
                .setEntityName(mapEntry.getValue()))
            .collect(Collectors.toList());

        Map<String, Double> currentQuotas = clientQuotaData.get(entity);
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

    private String sanitizeEntityName(String entityName) {
        if (entityName == null) {
            return "<default>";
        } else {
            return Sanitizer.desanitize(entityName);
        }
    }

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

    private ApiError entityToSanitizedUserClientId(ClientQuotaEntity entity, Map<String, String> sanitizedEntities) {
        if (entity.entries().isEmpty()) {
            return new ApiError(Errors.INVALID_REQUEST, "Invalid empty client quota entity");
        }

        for (Map.Entry<String, String> entityEntry : entity.entries().entrySet()) {
            String entityType = entityEntry.getValue();
            String entityName = entityEntry.getKey();
            String sanitizedEntityName = sanitizeEntityName(entityName);
            if (Objects.equals(entityType, ClientQuotaEntity.USER)) {
                sanitizedEntities.put(ClientQuotaEntity.USER, sanitizedEntityName);
            } else if (Objects.equals(entityType, ClientQuotaEntity.CLIENT_ID)) {
                sanitizedEntities.put(ClientQuotaEntity.CLIENT_ID, sanitizedEntityName);
            } else {
                throw new InvalidRequestException("Unhandled client quota entity type: " + entityType);
            }

            if (entityName != null && entityName.isEmpty()) {
                return new ApiError(Errors.INVALID_REQUEST, "Empty " + entityType + " not supported");
            }
        }

        return ApiError.NONE;
    }

    public void replay(QuotaRecord record) {
        ClientQuotaEntity entity = new ClientQuotaEntity(
            record.entity().stream()
                .collect(Collectors.toMap(QuotaRecord.EntityData::entityType, QuotaRecord.EntityData::entityName)));
        TimelineHashMap<String, Double> quotas = clientQuotaData.get(entity);
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
