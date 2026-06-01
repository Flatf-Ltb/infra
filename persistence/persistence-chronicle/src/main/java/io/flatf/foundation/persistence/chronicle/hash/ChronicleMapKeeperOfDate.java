package io.flatf.foundation.persistence.chronicle.hash;

import java.time.LocalDate;

import io.flatf.foundation.common.datetime.DateTimeUtil;
import io.flatf.foundation.common.lang.Validator;
import io.flatf.foundation.persistence.chronicle.exception.ChronicleIOException;
import net.openhft.chronicle.map.ChronicleMap;

import javax.annotation.Nonnull;

public final class ChronicleMapKeeperOfDate<K, V> extends ChronicleMapKeeper<K, V> {

	public ChronicleMapKeeperOfDate(ChronicleMapConfigurator<K, V> cfg) {
		super(cfg);
	}

	public ChronicleMap<K, V> acquire(@Nonnull LocalDate date) throws ChronicleIOException {
		Validator.nonNull(date, "date");
		return super.acquire(Integer.toString(DateTimeUtil.date(date)));
	}

}
