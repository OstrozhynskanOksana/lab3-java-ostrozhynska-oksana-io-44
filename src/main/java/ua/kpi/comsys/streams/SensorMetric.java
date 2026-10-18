package ua.kpi.comsys.streams;

import java.time.Instant;

public record SensorMetric(String deviceId, String metricType, double value, Instant timestamp) {
}

