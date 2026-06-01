package io.flatf.foundation.persistence.chronicle.hash;

import java.time.LocalDate;

import io.flatf.foundation.common.datetime.DateTimeUtil;
import io.flatf.foundation.common.lang.Validator;
import io.flatf.foundation.persistence.chronicle.exception.ChronicleIOException;
import net.openhft.chronicle.set.ChronicleSet;

public final class ChronicleSetKeeperOfDate<E> extends ChronicleSetKeeper<E> {

	public ChronicleSetKeeperOfDate(ChronicleSetConfigurator<E> cfg) {
		super(cfg);
	}

	public ChronicleSet<E> acquire(LocalDate date) throws ChronicleIOException {
		Validator.nonNull(date, "date");
		return super.acquire(Integer.toString(DateTimeUtil.date(date)));
	}

}
