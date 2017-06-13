package io.hzmsg.queue

import io.hzmsg.persist.MapDBQueueMessageStore
import io.moquette.interception.messages.HazelcastMessage
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.mapdb.DBMaker
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingDeque

/**
 * Created by ryan on 6/12/17.
 */
class RestoreQueueProcessorTests {
    companion object {
        private var mapDBQueueMessageStore: MapDBQueueMessageStore? = null

        @BeforeClass
        @JvmStatic
        fun beforeClass() {
            val db = DBMaker.fileDB("file.db").make()
            mapDBQueueMessageStore = MapDBQueueMessageStore(db, Executors.newScheduledThreadPool(1), 30)
            mapDBQueueMessageStore!!.init()
        }

        @AfterClass
        @JvmStatic
        fun afterClass() {
            mapDBQueueMessageStore!!.close()
        }
    }

    @Test
    fun testRestore() {
        for (i: Int in 1..100) {
            val hazelcastMessage = HazelcastMessage()
            hazelcastMessage.messageId = UUID.randomUUID().toString()
            hazelcastMessage.timestamp = System.currentTimeMillis()
            hazelcastMessage.data = "{'tradeNo':'20170612789321764372', 'enterTimestamp':'1497231477'}".toByteArray(Charsets.UTF_8)
//            print(hazelcastMessage.messageId)
            mapDBQueueMessageStore!!.put(hazelcastMessage.messageId, hazelcastMessage)
        }

        val blockingQueue: BlockingQueue<String> = LinkedBlockingDeque()

        val restoreQueueProcessor: RestoreQueueProcessor = RestoreQueueProcessor(mapDBQueueMessageStore, blockingQueue)
        restoreQueueProcessor.restore()

        assert(blockingQueue.size === 100)
    }
}