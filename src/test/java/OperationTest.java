import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;
import operation.OperationType;
import operation.OperationUtils;
import org.junit.Before;
import org.junit.Test;

public class OperationTest {

	private List<Object> values;

	@Before
	public void initialize() {
		values = new ArrayList<>();
		values.add(1);
		values.add(2);
		values.add(-5);
		values.add(7);
		values.add(9);
		values.add(4);
	}

	@Test
	public void testMin() {
		assertEquals(-5, OperationUtils.computeOperation(values, OperationType.MIN));
	}

	@Test
	public void testMax() {
		assertEquals(9, OperationUtils.computeOperation(values, OperationType.MAX));
	}

	@Test
	public void testAvg() {
		assertEquals(3, (double) OperationUtils.computeOperation(values, OperationType.AVG), 0.01);
	}

	@Test
	public void testSum() {
		assertEquals(18L, OperationUtils.computeOperation(values, OperationType.SUM));
	}

	@Test
	public void testCount() {
		assertEquals(6, (long) OperationUtils.computeOperation(values, OperationType.COUNT));
	}
}
