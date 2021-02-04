package operation;

import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalInt;

public class OperationUtils {
	public static Object computeOperation(List<Object> values, OperationType operationType) {
		try {
			switch (operationType) {
				case MIN:
					return computeMin(values);
				case MAX:
					return computeMax(values);
				case AVG:
					return computeAvg(values);
				case SUM:
					return computeSum(values);
				case COUNT:
					return values.stream().mapToInt(value -> (Integer) value).count();
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		return null;
	}


	public static Integer computeMin(List<Object> values) {
		OptionalInt optionalInt = values.stream().mapToInt(value -> (Integer) value).min();
		return optionalInt.isPresent() ? optionalInt.getAsInt() : null;
	}

	public static Integer computeMax(List<Object> values) {
		OptionalInt optionalInt = values.stream().mapToInt(value -> (Integer) value).max();
		return optionalInt.isPresent() ? optionalInt.getAsInt() : null;
	}

	public static Double computeAvg(List<Object> values) {
		OptionalDouble optionalDouble = values.stream().mapToInt(value -> (Integer) value).average();
		return optionalDouble.isPresent() ? optionalDouble.getAsDouble() : null;
	}

	public static Long computeSum(List<Object> values) {
		return values.stream().mapToLong(value -> Long.parseLong(String.valueOf(value))).sum();
	}
}
