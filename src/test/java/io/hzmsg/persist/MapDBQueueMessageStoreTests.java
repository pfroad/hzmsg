package io.hzmsg.persist;

import com.alibaba.fastjson.JSONObject;
import io.moquette.interception.messages.HazelcastMessage;
import kotlin.text.Charsets;
import org.junit.*;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2017/6/12.
 */
public class MapDBQueueMessageStoreTests {
    private static MapDBQueueMessageStore mapDBQueueMessageStore;

    @BeforeClass
    public static void beforeClass() {
        DB db = DBMaker.fileDB("file.db").make();
        mapDBQueueMessageStore = new MapDBQueueMessageStore(db, Executors.newScheduledThreadPool(1), 30);
        mapDBQueueMessageStore.init();
    }

    @Before
    public void before() {
        assert mapDBQueueMessageStore.getAll().size() == 0;
        HazelcastMessage hazelcastMessage = new HazelcastMessage();
        hazelcastMessage.setMessageId(UUID.randomUUID().toString());
        hazelcastMessage.setTimestamp(System.currentTimeMillis());
        hazelcastMessage.setData("{'tradeNo':'20170612789321764372', 'enterTimestamp':'1497231477'}".getBytes(Charsets.UTF_8));
        mapDBQueueMessageStore.put("20170612789321764372", hazelcastMessage);
    }

    @AfterClass
    public static void afterClass() {
        mapDBQueueMessageStore.close();
    }

//    @Test
//    public void testPut() {
//        assert mapDBQueueMessageStore.get("20170612789321764372") != null;
//    }

    @Test
    public void testGet() {
        HazelcastMessage hazelcastMessage = mapDBQueueMessageStore.get("20170612789321764372");
        System.out.println(hazelcastMessage.getMessageId());
        assert JSONObject.parseObject(new String(hazelcastMessage.getData(), Charsets.UTF_8)).get("tradeNo").equals("20170612789321764372");
    }

    @Test
    public void testGetAll() {
        assert mapDBQueueMessageStore.getAll().contains("20170612789321764372");
    }

    @After
    public void after() {
        mapDBQueueMessageStore.remove("20170612789321764372");
        assert mapDBQueueMessageStore.get("20170612789321764372") == null;
        assert mapDBQueueMessageStore.getAll().size() == 0;
    }
}
