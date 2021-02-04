package operation;

public class Operation {
	private OperationType operationType;
	private String column;

	public Operation(OperationType operationType, String column) {
		this.operationType = operationType;
		this.column = column;
	}

	public OperationType getOperationType() {
		return operationType;
	}

	public String getColumn() {
		return column;
	}
}
