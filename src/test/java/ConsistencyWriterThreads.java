import database.MyDatabase;
import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Logger;

public class ConsistencyWriterThreads extends Thread{

	private MyDatabase db;
	private CyclicBarrier barrier;
	private int threadId;
	private Logger logger = Logger.getLogger(ConsistencyWriterThreads.class.getName());

	public ConsistencyWriterThreads(MyDatabase db, CyclicBarrier barrier, int threadId)
	{
		this.db = db;
		this.barrier = barrier;
		this.threadId = threadId;
	}

	void barrierWrapper() {
		try
		{
			barrier.await();
		}
		catch (InterruptedException | BrokenBarrierException e)
		{
			e.printStackTrace();
		}
	}
	
	public void run()
	{
		logger.info("Inserting values");
		for(int i = 0; i < 1_000_000; i++)
		{
			ArrayList<Object> values = new ArrayList<>();
			values.add("Ion"+(i+(threadId*1_000_000)));
			values.add(i+(threadId*1_000_000));
			values.add(i+(threadId*1_000_000));
			values.add(i+(threadId*1_000_000));
			values.add(i+(threadId*1_000_000));
			db.insert("Students", values);
		}

		barrierWrapper();

		logger.info("Updating values");
		for(int i = 0; i < 10; i++)
		{
			ArrayList<Object> values = new ArrayList<Object>();
			values.add("George"+i);
			values.add(-i*threadId);
			values.add(-i*threadId);
			values.add(-i*threadId);
			values.add(-i*threadId);
			db.update("Students", values, "grade0 == "+i);
		}

		barrierWrapper();
	}
}
