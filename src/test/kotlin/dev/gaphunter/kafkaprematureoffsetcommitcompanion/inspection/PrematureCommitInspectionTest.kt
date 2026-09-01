package dev.gaphunter.kafkaprematureoffsetcommitcompanion.inspection

import com.intellij.testFramework.fixtures.BasePlatformTestCase

class PrematureCommitInspectionTest : BasePlatformTestCase() {

    override fun setUp() {
        super.setUp()
        myFixture.enableInspections(PrematureCommitInspection::class.java)
    }

    fun `test async dispatch with no explicit commit relies on risky auto-commit, is flagged`() {
        myFixture.configureByText(
            "Consumer.java",
            """
            class Consumer {
                void run(KafkaConsumer<String, String> consumer, ExecutorService executor) {
                    while (true) {
                        ConsumerRecords<String, String> records = consumer.poll(100);
                        for (ConsumerRecord<String, String> record : records) {
                            executor.submit(() -> process(record));
                        }
                    }
                }
                void process(ConsumerRecord<String, String> record) {}
            }
            """.trimIndent(),
        )
        val highlights = myFixture.doHighlighting()
        assertTrue(highlights.any { it.description?.contains("enable.auto.commit") == true })
    }

    fun `test async dispatch with an explicit commit right after is flagged`() {
        myFixture.configureByText(
            "Consumer2.java",
            """
            class Consumer2 {
                void run(KafkaConsumer<String, String> consumer, ExecutorService executor) {
                    while (true) {
                        ConsumerRecords<String, String> records = consumer.poll(100);
                        for (ConsumerRecord<String, String> record : records) {
                            executor.submit(() -> process(record));
                        }
                        consumer.commitSync();
                    }
                }
                void process(ConsumerRecord<String, String> record) {}
            }
            """.trimIndent(),
        )
        val highlights = myFixture.doHighlighting()
        assertTrue(highlights.any { it.description?.contains("without waiting for it to finish") == true })
    }

    fun `test async dispatch that is waited on before commit is not flagged`() {
        myFixture.configureByText(
            "Consumer3.java",
            """
            class Consumer3 {
                void run(KafkaConsumer<String, String> consumer, ExecutorService executor) throws Exception {
                    while (true) {
                        ConsumerRecords<String, String> records = consumer.poll(100);
                        Future<?> future = executor.submit(() -> process(records));
                        future.get();
                        consumer.commitSync();
                    }
                }
                void process(ConsumerRecords<String, String> records) {}
            }
            """.trimIndent(),
        )
        val highlights = myFixture.doHighlighting()
        assertTrue(highlights.none { it.description?.contains("finish") == true })
    }

    fun `test a synchronous poll-loop with no async dispatch at all is not flagged`() {
        myFixture.configureByText(
            "Consumer4.java",
            """
            class Consumer4 {
                void run(KafkaConsumer<String, String> consumer) {
                    while (true) {
                        ConsumerRecords<String, String> records = consumer.poll(100);
                        for (ConsumerRecord<String, String> record : records) {
                            process(record);
                        }
                        consumer.commitSync();
                    }
                }
                void process(ConsumerRecord<String, String> record) {}
            }
            """.trimIndent(),
        )
        val highlights = myFixture.doHighlighting()
        assertTrue(highlights.none { it.description?.contains("Kafka poll-loop") == true })
    }

    fun `test a loop with async dispatch but no poll call at all is not flagged`() {
        myFixture.configureByText(
            "NotAConsumer.java",
            """
            class NotAConsumer {
                void run(ExecutorService executor) {
                    while (true) {
                        executor.submit(() -> doWork());
                    }
                }
                void doWork() {}
            }
            """.trimIndent(),
        )
        val highlights = myFixture.doHighlighting()
        assertTrue(highlights.none { it.description?.contains("Kafka poll-loop") == true })
    }
}
