import database.Database;
import database.MyDatabase;
import database.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.junit.Test;

public class ScalabilityTest {

	private Logger logger = Logger.getLogger(ScalabilityTest.class.getName());

	@Test
	public void testScalability() {
		int[] nThreads = {1, 2, 4, 8};

		for(int numThreads : nThreads)
		{

			MyDatabase db = new Database();

			String[] columnNames = {"studentName", "grade", "hasGraduated"};
			Type[] columnTypes = {Type.STRING, Type.INT, Type.BOOL};

			db.createTable("Students", Arrays.asList(columnNames), Arrays.asList(columnTypes));
			db.initDb(numThreads);

			// inserting values
			for(int i = 0; i < 10_000_000; i++) {
				List<Object> values = new ArrayList<>();
				values.add("Ion" + i);
				values.add(i);
				values.add(i % 2 == 1);
				db.insert("Students", values);
			}

			long start = System.currentTimeMillis();
			String[] operations = {"grade"};
			List<List<Object>> results = db.select("Students", Arrays.asList(operations), "");
			long end = System.currentTimeMillis();

			logger.info("Selecting took " + (end - start));

			db.stopDb();
		}
	}
}
