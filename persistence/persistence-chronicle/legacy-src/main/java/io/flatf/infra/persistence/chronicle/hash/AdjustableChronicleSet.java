package io.flatf.infra.persistence.chronicle.hash;

import net.openhft.chronicle.set.ChronicleSet;

public record AdjustableChronicleSet<K>(
        ChronicleSet<K> entity) {
}
