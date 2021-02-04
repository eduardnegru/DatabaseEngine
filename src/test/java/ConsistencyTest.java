import database.Database;
import database.MyDatabase;
import database.Type;
import java.util.Arrays;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import org.junit.Test;

public class ConsistencyTest {
	@Test
	public void test() throws BrokenBarrierException, InterruptedException {

		MyDatabase db = new Database();
		String[] columnNames = {"studentName", "grade0", "grade1", "grade2","grade3"};
		Type[] columnTypes = {Type.STRING, Type.INT, Type.INT, Type.INT, Type.INT};
		db.createTable("Students", Arrays.asList(columnNames), Arrays.asList(columnTypes));
		int numThreads = 4;

		db.initDb(numThreads);

		CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
		ConsistencyWriterThreads[] threads = new ConsistencyWriterThreads[numThreads];
		ConsistencyReaderThread thread = new ConsistencyReaderThread(db, barrier);
		thread.start();

		for(int threadId = 1; threadId < numThreads; threadId++) {
			threads[threadId] = new ConsistencyWriterThreads(db, barrier, threadId);
			threads[threadId].start();
		}

		barrier.await();
		barrier.await();

		for(int threadId = 1; threadId < numThreads; threadId++)
		{
			threads[threadId].join();
		}

		thread.join();
		db.stopDb();
	}
}
