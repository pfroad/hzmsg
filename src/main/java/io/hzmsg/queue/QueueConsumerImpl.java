package io.hzmsg.queue;

import io.hzmsg.annotations.HazelcastQueueListener;
import io.hzmsg.persist.IMessageStore;
import io.hzmsg.persist.MapDBQueueMessageStore;
import io.hzmsg.persist.MemoryQueueMessageStore;
import io.hzmsg.queue.strategy.DefaultQueueFailedStrategy;
import io.hzmsg.queue.strategy.QueueFailedStrategy;
import io.hzmsg.queue.strategy.RetryQueueFailedStrategy;
import com.alibaba.fastjson.JSON;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import io.moquette.interception.messages.HazelcastMessage;
import org.apache.commons.lang3.StringUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class QueueConsumerImpl implements QueueConsumer {
	private static final Logger LOG = LoggerFactory.getLogger(QueueConsumerImpl.class);
	private static final String PERSIST_FILE = "queue.db";

	private HazelcastInstance hazelcast;
	private String queueName;
	private int poolSize = 1;
	private ExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
		private AtomicInteger count = new AtomicInteger();
		public Thread newThread(Runnable r) {
			int i = count.incrementAndGet();
			return new Thread(r, "QueueConsumerImplThreadExecutor" + i);
		}
	});

	private String persistPath;
	private IMessageStore<HazelcastMessage> messageStore;
	private int autoCommitInterval;

	private int receivedMsgQueueCapacity;
	private BlockingQueue<String> receivedMsgQueue;

	private ConcurrentMap<HZQueueListener, HazelcastQueueListener> listenerAnnoMap = new ConcurrentHashMap();
	private IAtomicLong atomicLong;

	private HZQueueReceiver hzQueueReceiver;
	private QueueConsumeProcessor queueConsumeProcessor;
	private final HZQueueListener hzQueueListener;
	private QueueFailedStrategy failedStrategy;

	public QueueConsumerImpl(HazelcastInstance hazelcast,
							 String queueName,
							 int poolSize,
							 int receivedMsgQueueCapacity,
							 HZQueueListener hzQueueListener,
							 QueueFailedStrategy failedStrategy) {
		this.hazelcast = hazelcast;
		this.queueName = queueName;
		this.poolSize = poolSize;
		this.receivedMsgQueueCapacity = receivedMsgQueueCapacity;
		this.hzQueueListener = hzQueueListener;
		this.failedStrategy = failedStrategy;
		this.receivedMsgQueue = new LinkedBlockingDeque<>(this.receivedMsgQueueCapacity);

		executorService = Executors.newFixedThreadPool(this.poolSize, new ThreadFactory() {
			private AtomicInteger count = new AtomicInteger();
			public Thread newThread(Runnable r) {
				int i = count.incrementAndGet();
				return new Thread(r, "QueueConsumerImplThreadExecutor" + i);
			}
		});
	}

	public QueueConsumerImpl(HazelcastInstance hazelcast,
							 String queueName,
							 int poolSize,
							 int receivedMsgQueueCapacity,
							 HZQueueListener hzQueueListener,
							 QueueFailedStrategy failedStrategy,
							 String persistPath,
							 int autoCommitInterval) {
		this.hazelcast = hazelcast;
		this.queueName = queueName;
		this.poolSize = poolSize;
		this.receivedMsgQueueCapacity = receivedMsgQueueCapacity;
		this.hzQueueListener = hzQueueListener;
		this.failedStrategy = failedStrategy;
		this.messageStore = new MemoryQueueMessageStore();
		this.persistPath = persistPath;
		this.autoCommitInterval = autoCommitInterval;
		this.receivedMsgQueue = new LinkedBlockingDeque<>(this.receivedMsgQueueCapacity);

		executorService = Executors.newFixedThreadPool(this.poolSize, new ThreadFactory() {
			private AtomicInteger count = new AtomicInteger();
			public Thread newThread(Runnable r) {
				int i = count.incrementAndGet();
				return new Thread(r, "QueueConsumerImplThreadExecutor" + i);
			}
		});
	}

	public void start() {
		LOG.info("Starting consumer. queue={}", queueName);
		if (failedStrategy == null) {
			failedStrategy = new DefaultQueueFailedStrategy();
		} else if (failedStrategy instanceof RetryQueueFailedStrategy) {
			((RetryQueueFailedStrategy) failedStrategy).setQueueConsumer(this);
		}

		if (StringUtils.isNotEmpty(this.persistPath)) {
			File persistPath = new File(this.persistPath);
			if (!persistPath.exists()) {
				persistPath.mkdirs();
			}

			DB db = DBMaker.fileDB(getPersistFile()).make();
			this.messageStore = new MapDBQueueMessageStore(db, Executors.newScheduledThreadPool(1), this.autoCommitInterval);
		} else {
			this.messageStore = new MemoryQueueMessageStore();
		}

		this.messageStore.init();

		// restore received message which is not been handled
		RestoreQueueProcessor restoreQueueProcessor = new RestoreQueueProcessor(messageStore, receivedMsgQueue);
		restoreQueueProcessor.restore();

		this.hzQueueReceiver = new HZQueueReceiver(this.messageStore, this.receivedMsgQueue, this.hzQueueListener, this.hazelcast, this.queueName);
		this.hzQueueReceiver.start();

		this.queueConsumeProcessor = new QueueConsumeProcessor(messageStore, receivedMsgQueue, this);
		this.queueConsumeProcessor.start();
	}

	public void shutdown() {
		LOG.info("Shutdown Queue consumer......");
		if (hzQueueReceiver != null) {
			hzQueueReceiver.shutdown();
		}

		if (this.queueConsumeProcessor != null) {
			this.queueConsumeProcessor.shutdown();
		}

		executorService.shutdown();

		if (messageStore != null) {
			messageStore.close();
		}
		LOG.info("End to shutdown.");
	}

	public void consume(final HazelcastMessage hazelcastMessage) {
		executorService.execute(new Runnable() {

			public void run() {
				LOG.info("Consume queue message {}", new String(hazelcastMessage.getData(), Charset.forName("UTF-8")));
				try {
					hzQueueListener.handle(hazelcastMessage);
					// remove store message
					messageStore.remove(hazelcastMessage.getMessageId());
				} catch (Exception e) {
					LOG.error("Failed handle queue.", e);
					try {
						failedStrategy.handleFailed(hazelcastMessage);
					} catch (InterruptedException e1) {
						LOG.error("Failed handle is failed.", e);
					}
				}

			}

		});
	}
	
	public HazelcastInstance getHazelcast() {
		return hazelcast;
	}

	public void setHazelcast(HazelcastInstance hazelcast) {
		this.hazelcast = hazelcast;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public boolean retry(final HazelcastMessage message) {
		LOG.info("Retry failed message:{}", JSON.toJSONString(message));

		HazelcastQueueListener anno = listenerAnnoMap.get(hzQueueListener);

		if (anno == null) {
			anno = hzQueueListener.getClass().getAnnotation(HazelcastQueueListener.class);

			if (anno != null)
				listenerAnnoMap.put(hzQueueListener, anno);
		}

		if (anno != null) {
			IAtomicLong atomicLong = hazelcast.getAtomicLong(message.getMessageId());
			if (atomicLong.incrementAndGet() <= anno.retry()) {
				while (true) {
					try {
						if (this.receivedMsgQueue.offer(message.getMessageId(), 1000, TimeUnit.MILLISECONDS)) {
							break;
						}
					} catch (InterruptedException e) {
					}
				}
				return true;
			}
		}

		return false;
	}

	private String getPersistFile() {
		StringBuilder persistFile = new StringBuilder();
		persistFile.append(this.persistPath);
		persistFile.append("/");
		persistFile.append(this.queueName.replaceAll("/", "-"));
		persistFile.append("-");
		persistFile.append(PERSIST_FILE);
		return persistFile.toString();
	}

	public QueueFailedStrategy getFailedStrategy() {
		return failedStrategy;
	}

	public void setFailedStrategy(QueueFailedStrategy failedStrategy) {
		this.failedStrategy = failedStrategy;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}
}
