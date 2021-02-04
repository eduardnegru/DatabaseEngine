import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;
import operation.OperationUtils;
import org.junit.Before;
import org.junit.Test;

public class OperationUtilsTest {

	private List<Object> values = new ArrayList<>();

	@Before
	public void initialize() {
		values = new ArrayList<>();
		values.add(1);
		values.add(2);
		values.add(3);
		values.add(4);
		values.add(5);
		values.add(6);
		values.add(7);
		values.add(8);
	}

	@Test
	public void minTest() {
		Object result = OperationUtils.computeMin(values);
		assertEquals(1, result);
	}

	@Test
	public void maxTest() {
		Object result = OperationUtils.computeMax(values);
		assertEquals(8, result);
	}

	@Test
	public void sumTest() {
		Object result = OperationUtils.computeSum(values);
		assertEquals(36L, result);
	}

	@Test
	public void avgTest() {
		Object result = OperationUtils.computeAvg(values);
		assertEquals(4.5, result);
	}
}
