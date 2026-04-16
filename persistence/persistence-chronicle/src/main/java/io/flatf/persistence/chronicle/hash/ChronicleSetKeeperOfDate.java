package io.flatf.persistence.chronicle.hash;

import java.time.LocalDate;

import io.flatf.common.datetime.DateTimeUtil;
import io.flatf.common.lang.Validator;
import io.flatf.persistence.chronicle.exception.ChronicleIOException;
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
