package io.flatf.persistence.chronicle.hash;

import net.openhft.chronicle.map.ChronicleMap;

public record AdjustableChronicleMap<K, V>(
        ChronicleMap<K, V> entity) {
}