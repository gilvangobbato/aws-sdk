package com.github.gilvangobbato.entity;

import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

public class LocalDateTimeConverter implements AttributeConverter<LocalDateTime> {

    @Override
    public AttributeValue transformFrom(LocalDateTime input) {
        if(input == null) return AttributeValue.fromNul(Boolean.TRUE);
        return AttributeValue.fromN(Objects.toString(input.toInstant(ZoneOffset.UTC).toEpochMilli()));
    }

    @Override
    public LocalDateTime transformTo(AttributeValue input) {
        if(input == null || input.n() == null || input.n().isBlank()) return null;
        return Instant.ofEpochMilli(Long.parseLong(Objects.toString(input.n())))
                .atOffset(ZoneOffset.UTC).toLocalDateTime();
    }

    @Override
    public EnhancedType<LocalDateTime> type() {
        return EnhancedType.of(LocalDateTime.class);
    }

    @Override
    public AttributeValueType attributeValueType() {
        return AttributeValueType.N;
    }
}
