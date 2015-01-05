package chaptor0203.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class CalculateCache {
	// 计算最终的指定时间范围内的数据个数
	private Thread calculateThread;

	// 统计个数用
	AtomicInteger count;

	ReentrantLock lock = new ReentrantLock();

	public CalculateCache(final int timeSection, final ExpiredCallback callback) {
		this.count = new AtomicInteger(0);

		this.calculateThread = new Thread(new Runnable() {
			@SuppressWarnings("static-access")
			@Override
			public void run() {
				try {
					while (true) {
						// Thread.currentThread().sleep(timeSection * 60 *
						// 1000);
						Thread.currentThread().sleep(10 * 1000);
						lock.lock();
						try {
							callback.expire(count);

						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							lock.unlock();
						}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		this.calculateThread.setDaemon(true);
		this.calculateThread.start();

	}

	public void put(int newData) {
		count.incrementAndGet();
	}

	public static interface ExpiredCallback {
		void expire(AtomicInteger currentValue);
	}

}
