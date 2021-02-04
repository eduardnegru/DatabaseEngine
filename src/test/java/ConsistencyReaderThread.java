import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import database.MyDatabase;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Logger;

public class ConsistencyReaderThread extends Thread
{

	private MyDatabase db;
	private CyclicBarrier barrier;
	private Logger logger = Logger.getLogger(ConsistencyReaderThread.class.getName());

	public ConsistencyReaderThread(MyDatabase db, CyclicBarrier barrier)
	{
		this.db = db;
		this.barrier = barrier;
	}
	
	void barrierWrapper()
	{
		try
		{
			barrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}
	}
	
	public void run()
	{
		List<List<Object>> results = null;
		String[] operations = {"sum(grade0)", "sum(grade1)", "sum(grade2)", "sum(grade3)"};
		results = db.select("Students", Arrays.asList(operations), "");

		// select & insert consistency
		boolean passed = true;
		for(int i = 0; i < 100; i++)
		{
			results = db.select("Students", Arrays.asList(operations), "");
			for(int index=1; index<results.size(); index++)
			{
				if (!results.get(0).isEmpty() && !results.get(index).isEmpty() && (long) results.get(index).get(0) != (long) results.get(0).get(0))
				{
					logger.warning("Select/Insert Consistency FAIL" + results.get(index).get(0) + " " + results.get(0).get(0));
					passed = false;
				}
			}
		}
		assertTrue(passed);

		logger.info("Select/Insert Consistency PASS");

		barrierWrapper();

		// select & update consistency
		passed = true;

		for(int i = 0; i < 20; i++)
		{
			results = db.select("Students", Arrays.asList(operations), "grade0 > -1");
			for(int index=1; index<results.size(); index++)
			{
				if (!results.get(0).isEmpty() && !results.get(index).isEmpty() && (long) results.get(index).get(0) != (long) results.get(0).get(0))
				{
					logger.warning("Select/Update Consistency FAIL" + results.get(index).get(0) + " " + results.get(0).get(0));
					passed = false;
				}
			}
		}

		assertTrue(passed);
		logger.info("Select/Update Consistency PASS");
		barrierWrapper();
	}

}
