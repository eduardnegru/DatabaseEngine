import static org.junit.Assert.assertEquals;
import database.Database;
import database.MyDatabase;
import database.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelectTest {
	private MyDatabase db = new Database();
	private final int rowCount = 10;
	private  int randomRow = new Random().nextInt(rowCount);

	@Before
	public void initialize() {
		String[] columnNames = {"studentName", "grade", "hasGraduated"};
		Type[] columnTypes = {Type.STRING, Type.INT, Type.BOOL};

		db.createTable("Students", Arrays.asList(columnNames), Arrays.asList(columnTypes));

		db.initDb(1);

		for(int i = 0; i < rowCount; i++) {
			ArrayList<Object> values = new ArrayList<>();
			values.add("John" + i);
			values.add(i);
			values.add(i % 2 == 1);
			db.insert("Students", values);
		}

		System.out.println("Random = " + randomRow);
	}

	@Test
	public void selectWithFilterEquals() {
		String[] operations = {"studentName", "grade", "hasGraduated"};
		List<List<Object>> results = db.select("Students", Arrays.asList(operations), "grade == " + randomRow);
		System.out.println(results);
		assertEquals("John" + randomRow, results.get(0).get(0));
		assertEquals(randomRow, results.get(1).get(0));
		assertEquals(randomRow % 2 == 1, results.get(2).get(0));
	}

	@Test
	public void selectWithFilterGreaterThan() {
		String[] operations = {"studentName", "grade", "hasGraduated"};
		List<List<Object>> results = db.select("Students", Arrays.asList(operations), "grade > " + randomRow);
		System.out.println(results);
		int expectedSize = rowCount - randomRow - 1;
		assertEquals(expectedSize, results.get(0).size());
		assertEquals(expectedSize, results.get(1).size());
		assertEquals(expectedSize, results.get(2).size());
		if (randomRow != rowCount - 1)
			assertEquals("John" + (randomRow + 1), results.get(0).get(0));
	}

	@Test
	public void selectWithFilterLessThan() {
		String[] operations = {"studentName", "grade", "hasGraduated"};
		System.out.println(randomRow);
		List<List<Object>> results = db.select("Students", Arrays.asList(operations), "grade < " + randomRow);
		System.out.println(results);
		assertEquals(randomRow, results.get(0).size());
		assertEquals(randomRow, results.get(1).size());
		assertEquals(randomRow, results.get(2).size());

		if (randomRow != 0)
			assertEquals("John" + (randomRow - 1), results.get(0).get(results.get(0).size() - 1));

	}

	@Test
	public void selectMin() {
		String[] operations = {"min(grade)"};
		List<List<Object>> results = db.select("Students", Arrays.asList(operations), "");
		assertEquals(0, results.get(0).get(0));
	}

	@Test
	public void selectMax() {
		String[] operations = {"max(grade)"};
		List<List<Object>> results = db.select("Students", Arrays.asList(operations), "");
		assertEquals(rowCount - 1, results.get(0).get(0));
	}

	@Test
	public void selectSum() {
		String[] operations = {"sum(grade)"};
		System.out.println(randomRow);
		List<List<Object>> results = db.select("Students", Arrays.asList(operations), "");
		assertEquals((rowCount - 1) * (long)rowCount / 2, results.get(0).get(0));
	}

	@Test
	public void selectAvg() {
		String[] operations = {"avg(grade)"};
		System.out.println(randomRow);
		List<List<Object>> results = db.select("Students", Arrays.asList(operations), "");
		assertEquals((rowCount - 1) * rowCount / (double)(2 * rowCount), (Double) results.get(0).get(0), 0.01);
	}

	@Test
	public void selectCount() {
		String[] operations = {"count(grade)"};
		List<List<Object>> results = db.select("Students", Arrays.asList(operations), "");
		assertEquals(rowCount, (long) results.get(0).get(0));
	}

	@Test
	public void selectAll() {
		String[] operations = {"studentName", "grade", "hasGraduated"};
		List<List<Object>> results = db.select("Students", Arrays.asList(operations), "");
		assertEquals(rowCount, results.get(0).size());
		assertEquals(rowCount, results.get(1).size());
		assertEquals(rowCount, results.get(2).size());
	}

	@After
	public void cleanup() {
		db.stopDb();
	}
}
