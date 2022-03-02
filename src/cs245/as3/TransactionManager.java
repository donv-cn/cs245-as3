package cs245.as3;

import java.awt.desktop.AppReopenedEvent;
import java.nio.ByteBuffer;
import java.util.*;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 *
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {

	class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}

	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> lastValues;

	/**
	 * 数据库
	 * */
	private StorageManager storage;

	/**
	 * 日志记录文件
	 * */
	private LogManager log;

	/**
	 * 日志偏移量 tag 唯一对应的 key,维护日志偏移量的最小值,用于在数据持久化过程中合理设置日志截断点.
	 * */
	private TreeMap<Long,Long> tagWithKey;

	/**
	 * key 映射到 key 的最新值的日志偏移量 tag.
	 * */
	private HashMap<Long,Long> keyWithTag;

	/**
	 * Hold on to writesets until commit.
	 */
	private HashMap<Long, ArrayList<WritesetEntry>> writesets;

	public TransactionManager() {
		writesets = new HashMap<>();
		tagWithKey = new TreeMap<>();
		keyWithTag = new HashMap<>();

		// see initAndRecover
		lastValues = null;
		storage = null;
		log = null;
	}

	/**
	* 对数据进行序列化.
	* */
	private byte[] serialize(int len, long key, byte[] value) {
		ByteBuffer ret = ByteBuffer.allocate(Short.BYTES + Long.BYTES + value.length);

		/* 第一,二，三个字段分别记录 [ value 的长度 | key 的值 | value 的值 ].
		*                           short        long       byte[]
		* */
		ret.putShort((short)len);
		ret.putLong(key);
		ret.put(value);
		return ret.array();
	}

	/**
	 * 反序列化，返回 [value 长度，key 值].
	 * */
	private long[] lengthOfValueAndKeyDeserialize(byte[] b) {
		ByteBuffer wrap = ByteBuffer.wrap(b);
		long len = wrap.getShort();
		long key = wrap.getLong();
		return new long[]{len,key};
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {
		lastValues = sm.readStoredTable();
		storage = sm;
		log = lm;

		// 日志截断点偏移量.
		int logStartOffset = log.getLogTruncationOffset();

		// 日志总偏移量.
		int logEndOffset = log.getLogEndOffset();

		// 判断日志是否有效.
		if (logStartOffset >= logEndOffset) {
			return;
		}

		// 记录日志中最新 (key, value).
		Map<Long,byte[]> logLastValues = new HashMap<>();
		// 临时变量，辅助判断事务日志是否完整.
		Map<Long,byte[]> temp = new HashMap<>();

		while (logStartOffset < logEndOffset) {

			// 获取取 value 长度 和 key.
			byte[] len_key_bytes = log.readLogRecord(logStartOffset, Short.BYTES + Long.BYTES);
			long[] nums = lengthOfValueAndKeyDeserialize(len_key_bytes);
			logStartOffset += (Short.BYTES + Long.BYTES);

			int len = (int)nums[0];
			long key = nums[1];

			// 获取 value.
			byte[] value = log.readLogRecord(logStartOffset, len);
			logStartOffset += len;

			// 判断写日志的事务是否有结束标志.
			if (Arrays.equals(value,"transactionEnd".getBytes())) {
				logLastValues.putAll(temp);
			} else {
				temp.put(key,value);
			}
		}

		// 日志设置新的截断点.
		log.setLogTruncationOffset(logEndOffset);

		// 对于日志中需要持久化，但没有持久化的数据进行重做事务处理.
		for (Map.Entry<Long,byte[]> entry : logLastValues.entrySet()) {
			long k = entry.getKey();
			byte[] v = entry.getValue();

			byte[] inform = serialize(v.length, k, v);
			long tag = log.appendLogRecord(inform);
			storage.queueWrite(k,tag,v);

			TaggedValue tv = new TaggedValue(tag,v);

			lastValues.put(k, tv);

			// 更新维护需持久化 key 映射的日志偏移量 tag.
			keyWithTag.put(k,tag);
			tagWithKey.put(tag,k);
		}
		// 把以上当成一个事务处理,需要加入一个事务结束的标志.
		byte[] end = "transactionEnd".getBytes();
		byte[] serializeEnd = serialize(end.length, 502L, end);
		log.appendLogRecord(serializeEnd);
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txID) {
		writesets.put(txID,new ArrayList<>());
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 */
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = lastValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read() 
	 * calls until the transaction making the write commits. For simplicity, we will not make reads 
	 * to this same key from txID itself after we make a write to the key. 
	 */
	public void write(long txID, long key, byte[] value) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);

		if (writeset != null) {
			writeset.add(new WritesetEntry(key, value));
		}
	}

	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		// 记录事务完整执行的数据.
		HashMap<Long,TaggedValue> commitValues = new HashMap<>();

		if (writeset != null) {
			for (WritesetEntry x : writeset) {
				byte[] logInform = serialize(x.value.length,x.key, x.value);

				// 记录添加日志信息时的偏移量.
				long tag = log.appendLogRecord(logInform);
				commitValues.put(x.key, new TaggedValue(tag,x.value));
			}
			// 事务结束标志,如果在日志中没有事务结束标志,说明发生了日志崩溃.
			byte[] end = "transactionEnd".getBytes();
			byte[] serializeEnd = serialize(end.length, 502L, end);
			log.appendLogRecord(serializeEnd);

			for (Map.Entry<Long,TaggedValue> entry : commitValues.entrySet()) {
				long key = entry.getKey();
				TaggedValue tv = entry.getValue();

				storage.queueWrite(key,tv.tag,tv.value);
				lastValues.put(key, tv);

				// 检查更新 key 对应的 日志偏移量 tag,同时也检查它的辅助哈希表.
				if (keyWithTag.containsKey(key)) {
					long tag = keyWithTag.get(key);
					tagWithKey.remove(tag);
				}
				keyWithTag.put(key,tv.tag);
				tagWithKey.put(tv.tag,key);
			}
			writesets.remove(txID);
		}
	}

	/**
	 * Aborts a transaction.
	 */
	public void abort(long txID) {
		writesets.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
	public void writePersisted(long persisted_key, long persisted_tag, byte[] persisted_value) {
		// 删除当前已经持久化的 key.
		tagWithKey.remove(persisted_tag);
		keyWithTag.remove(persisted_key);
		if (!tagWithKey.isEmpty()) {
			long tag = tagWithKey.firstKey();
			// 日志截断到事务成功日志偏移量的最小值处.
			log.setLogTruncationOffset((int)tag);
		}
	}
}
