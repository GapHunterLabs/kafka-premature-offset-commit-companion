import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

class Consumer {

    // Flagged: async dispatch, no explicit commit -- relies on risky
    // enable.auto.commit=true default.
    void runUnsafe(KafkaConsumer<String, String> consumer, ExecutorService executor) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                executor.submit(() -> process(record));
            }
        }
    }

    // Flagged: commits immediately after dispatching, without waiting.
    void runUnsafeExplicit(KafkaConsumer<String, String> consumer, ExecutorService executor) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                executor.submit(() -> process(record));
            }
            consumer.commitSync();
        }
    }

    // Not flagged: waits for the async work before committing.
    void runSafe(KafkaConsumer<String, String> consumer, ExecutorService executor) throws Exception {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            Future<?> future = executor.submit(() -> process(records));
            future.get();
            consumer.commitSync();
        }
    }

    void process(Object record) {}
}
