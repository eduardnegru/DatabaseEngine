import static org.junit.Assert.assertEquals;
import database.Database;
import database.MyDatabase;
import database.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;

public class UpdateTest {
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
	public void updateWithoutFilter() {
		ArrayList<Object> newValues = new ArrayList<>();
		newValues.add("UpdatedJohn");
		newValues.add(11);
		newValues.add(false);

		db.update("Students", newValues, "grade > " + randomRow);

		String[] columnNames = {"studentName", "grade", "hasGraduated"};
		List<List<Object>> results = db.select("Students", Arrays.asList(columnNames), "");

		for (int i = randomRow + 1; i < rowCount; i++) {
			assertEquals("UpdatedJohn", results.get(0).get(i));
			assertEquals(11, results.get(1).get(i));
			assertEquals(false, results.get(2).get(i));
		}
	}
}
