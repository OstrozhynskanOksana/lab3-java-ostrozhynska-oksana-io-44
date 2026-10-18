package ua.kpi.comsys.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class TelemetryStreamAnalyzerTest {

    private static final Instant TIMESTAMP = Instant.parse("2026-01-01T00:00:00Z");
    private final TelemetryStreamAnalyzer analyzer = new TelemetryStreamAnalyzer();

    @Test
    @Timeout(2)
    void findFirstAnomalyStopsOnInfiniteStream() {
        AtomicInteger generated = new AtomicInteger();
        Stream<SensorMetric> stream = Stream.generate(() -> {
            int index = generated.getAndIncrement();
            return metric("device-1", "temperature", index == 4 ? 80 : 20 + index, index);
        });

        Optional<SensorMetric> result = analyzer.findFirstAnomaly(stream, "device-1", 70);

        assertTrue(result.isPresent());
        assertEquals(80, result.orElseThrow().value());
        assertEquals(5, generated.get());
    }

    @Test
    @Timeout(2)
    void streamActiveMetricsSkipsStablePrefixAndLimitsInfiniteStream() {
        AtomicInteger generated = new AtomicInteger();
        Stream<SensorMetric> stream = Stream.iterate(0, index -> index + 1)
                .map(index -> {
                    generated.incrementAndGet();
                    return metric("device-1", "temperature", index < 3 ? 10 : index + 10, index);
                });

        List<SensorMetric> result = analyzer.streamActiveMetrics(stream, 10, 4);

        assertEquals(List.of(13.0, 14.0, 15.0, 16.0),
                result.stream().map(SensorMetric::value).toList());
        assertEquals(7, generated.get());
    }

    @Test
    @Timeout(2)
    void calculateMovingAveragesUsesCustomGathererOnInfiniteStream() {
        AtomicInteger generated = new AtomicInteger();
        Stream<SensorMetric> stream = Stream.generate(() -> {
            int index = generated.getAndIncrement();
            return metric("device-1", "temperature", index + 1, index);
        });

        List<Double> result = analyzer.calculateMovingAverages(stream, "temperature", 3, 5);

        assertEquals(List.of(1.0, 1.5, 2.0, 3.0, 4.0), result);
        assertEquals(5, generated.get());
    }

    @Test
    void calculateMovingAveragesFiltersMetricType() {
        List<SensorMetric> metrics = List.of(
                metric("device-1", "humidity", 90, 0),
                metric("device-1", "temperature", 10, 1),
                metric("device-1", "temperature", 20, 2)
        );

        assertEquals(List.of(10.0, 15.0),
                analyzer.calculateMovingAverages(metrics.stream(), "temperature", 2, 10));
    }

    @Test
    void nullRequiredArgumentsThrowIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.findFirstAnomaly(null, "device-1", 10));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.findFirstAnomaly(Stream.empty(), null, 10));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.streamActiveMetrics(null, 10, 1));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.calculateMovingAverages(null, "temperature", 2, 1));
        assertThrows(IllegalArgumentException.class,
                () -> analyzer.calculateMovingAverages(Stream.empty(), null, 2, 1));
    }

    private static SensorMetric metric(String deviceId, String metricType, double value, long seconds) {
        return new SensorMetric(deviceId, metricType, value, TIMESTAMP.plusSeconds(seconds));
    }
}

