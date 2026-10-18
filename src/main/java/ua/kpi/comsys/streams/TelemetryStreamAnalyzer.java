package ua.kpi.comsys.streams;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Gatherer;
import java.util.stream.Stream;

public class TelemetryStreamAnalyzer {

    public Optional<SensorMetric> findFirstAnomaly(
            Stream<SensorMetric> stream,
            String targetDeviceId,
            double criticalThreshold
    ) {
        requireStream(stream);
        requireArgument(targetDeviceId, "targetDeviceId");

        return stream
                .filter(Objects::nonNull)
                .filter(metric -> targetDeviceId.equals(metric.deviceId()))
                .filter(metric -> metric.value() > criticalThreshold)
                .findFirst();
    }

    public List<SensorMetric> streamActiveMetrics(
            Stream<SensorMetric> stream,
            double normalThreshold,
            int maxCount
    ) {
        requireStream(stream);
        if (maxCount < 0) {
            throw new IllegalArgumentException("maxCount must not be negative");
        }

        return stream
                .dropWhile(metric -> metric != null && metric.value() <= normalThreshold)
                .limit(maxCount)
                .toList();
    }

    public List<Double> calculateMovingAverages(
            Stream<SensorMetric> stream,
            String metricType,
            int windowSize,
            int outputLimit
    ) {
        requireStream(stream);
        requireArgument(metricType, "metricType");
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize must be positive");
        }
        if (outputLimit < 0) {
            throw new IllegalArgumentException("outputLimit must not be negative");
        }
        if (outputLimit == 0) {
            return List.of();
        }

        return stream
                .filter(Objects::nonNull)
                .filter(metric -> metricType.equals(metric.metricType()))
                .sequential()
                .gather(movingAverageGatherer(windowSize, outputLimit))
                .toList();
    }

    private static Gatherer<SensorMetric, MovingAverageState, Double> movingAverageGatherer(
            int windowSize,
            int outputLimit
    ) {
        return Gatherer.<SensorMetric, MovingAverageState, Double>ofSequential(
                () -> new MovingAverageState(windowSize),
                (state, metric, downstream) -> {
                    if (state.outputCount >= outputLimit) {
                        return false;
                    }

                    state.add(metric.value());
                    state.outputCount++;
                    boolean accepted = downstream.push(state.average());
                    return accepted && state.outputCount < outputLimit;
                }
        );
    }

    private static void requireStream(Stream<?> stream) {
        if (stream == null) {
            throw new IllegalArgumentException("stream must not be null");
        }
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }

    private static final class MovingAverageState {
        private final int windowSize;
        private final ArrayDeque<Double> window = new ArrayDeque<>();
        private double sum;
        private int outputCount;

        private MovingAverageState(int windowSize) {
            this.windowSize = windowSize;
        }

        private void add(double value) {
            window.addLast(value);
            sum += value;
            if (window.size() > windowSize) {
                sum -= window.removeFirst();
            }
        }

        private double average() {
            return sum / window.size();
        }
    }
}

