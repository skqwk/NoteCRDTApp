package shared.crdt.hlc;

import java.time.Instant;

public class HybridTimestamp implements Comparable<HybridTimestamp> {
    private final long wallClockTime;
    private final int ticks;
    private final String nodeId;

    public HybridTimestamp(long wallClockTime,
                           int ticks,
                           String nodeId) {
        this.wallClockTime = wallClockTime;
        this.ticks = ticks;
        this.nodeId = nodeId;
    }


    @Override
    public String toString() {
        return String.format("%s-%s-%s",
                Instant.ofEpochMilli(wallClockTime).toString(),
                Integer.toString(ticks, 16),
                nodeId
        );
    }

    @Override
    public int compareTo(HybridTimestamp other) {
        if (this.wallClockTime == other.getWallClockTime()) {
            return Integer.compare(this.ticks, other.ticks);
        }
        if (this.wallClockTime > other.wallClockTime) {
            return 1;
        } else {
            return -1;
        }
    }

    public long getWallClockTime() {
        return wallClockTime;
    }

    public HybridTimestamp addTicks(int ticks) {
        return new HybridTimestamp(
                wallClockTime,
                this.ticks + ticks,
                nodeId);
    }

    public HybridTimestamp max(HybridTimestamp other) {
        if (this.wallClockTime == other.wallClockTime) {
            return this.ticks > other.ticks ? this : other;
        }
        return (this.wallClockTime > other.wallClockTime) ? this : other;
    }

    public static HybridTimestamp parse(String timestamp) {
        int counterDash = timestamp.indexOf('-', timestamp.lastIndexOf(':'));
        int nodeIdDash = timestamp.indexOf('-', counterDash + 1);

        long wallClockTime = Instant.parse(timestamp.substring(0, counterDash)).toEpochMilli();
        int ticks = Integer.parseInt(timestamp.substring(counterDash + 1, nodeIdDash),16);
        String nodeId = timestamp.substring(nodeIdDash + 1);

        return new HybridTimestamp(wallClockTime, ticks, nodeId);
    }

    // инициализируем с -1, так что addTicks() вернет к 0
    public static HybridTimestamp fromSystemTime(Long systemTime, String nodeId) {
        return new HybridTimestamp(systemTime, -1, nodeId);
    }
}
