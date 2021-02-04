package database;

public enum Symbol {
	EQUALS("=="),
	LESS_THAN("<"),
	GREATER_THAN(">");

	private String symbol;

	Symbol(String symbol) {
		this.symbol = symbol;
	}

	public String getSymbol() {
		return symbol;
	}
}
