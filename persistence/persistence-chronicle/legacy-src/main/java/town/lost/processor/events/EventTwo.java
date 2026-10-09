package town.lost.processor.events;

public class EventTwo extends AbstractEvent<EventTwo> {
	
	String symbol;
	double price;
	double quantity;

	public String symbol() {
		return symbol;
	}

	public EventTwo symbol(String symbol) {
		this.symbol = symbol;
		return this;
	}

	public double price() {
		return price;
	}

	public EventTwo price(double price) {
		this.price = price;
		return this;
	}

	public double quantity() {
		return quantity;
	}

	public EventTwo quantity(double quantity) {
		this.quantity = quantity;
		return this;
	}

	@Override
	protected EventTwo self() {
		return this;
	}
}
