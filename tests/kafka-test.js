import { check, sleep } from "k6";
import { Connection, Writer, Reader, SchemaRegistry, SCHEMA_TYPE_JSON } from "k6/x/kafka";

// 1. Connection settings
const BROKERS = [__ENV.KAFKA_BROKERS || "localhost:9092"];
const TOPIC = "xk6-performance-benchmark";

// 2. Initialize the Schema Registry client
const schemaRegistry = new SchemaRegistry();

// 3. Define the load test parameters using k6 options
export const options = {
  scenarios: {
    kafka_throughput_test: {
      executor: "constant-vus",
      vus: 5,               // Adjust VUs to scale the load
      duration: "10s",       // Run test for 10 seconds
      gracefulStop: "2s",
    },
  },
  thresholds: {
    kafka_writer_error_count: ["count == 0"],
    kafka_reader_error_count: ["count == 0"],
  },
};

// 4. SETUP PHASE: Runs ONCE before VUs start.
export function setup() {
  const connection = new Connection({ address: BROKERS[0] });
  
  console.log(`[SETUP] Ensuring topic '${TOPIC}' exists...`);
  connection.createTopic({
    topic: TOPIC,
    numPartitions: 3,
    replicationFactor: 1,
  });

  connection.close();
  sleep(2); // Wait briefly for partition mapping to propagate
}

// 5. VU ITERATION PHASE
export default function () {
  const writer = new Writer({ brokers: BROKERS, topic: TOPIC });
  const reader = new Reader({ brokers: BROKERS, topic: TOPIC, partition: 0, offset: 0 });

  const messageKey = { id: `key-${__VU}-${__ITER}` };
  const messageValue = {
    payload: "Performance Testing Serialization",
    timestamp: Date.now(),
  };

  // Produce message to Kafka
  writer.produce({
    messages: [
      {
        key: schemaRegistry.serialize({
          data: messageKey,
          schemaType: SCHEMA_TYPE_JSON,
        }),
        value: schemaRegistry.serialize({
          data: messageValue,
          schemaType: SCHEMA_TYPE_JSON,
        }),
      },
    ],
  });

  // Consume message from Kafka
  const receivedMessages = reader.consume({ limit: 1 });

  // Validate that message was written and read successfully
  check(receivedMessages, {
    "Message received successfully": (msgs) => msgs && msgs.length > 0,
    "Payload verification matches string": (msgs) => {
      if (!msgs || msgs.length === 0) return false;
      const deserialized = schemaRegistry.deserialize({
        data: msgs[0].value,
        schemaType: SCHEMA_TYPE_JSON,
      });
      return deserialized.payload === "Performance Testing Serialization";
    },
  });

  // Clean up connections
  writer.close();
  reader.close();

  sleep(1);
}

// 6. TEARDOWN PHASE: Runs ONCE at the end of the test.
export function teardown() {
  console.log("[TEARDOWN] Test complete. Cleaning up cluster topics...");
  const connection = new Connection({ address: BROKERS[0] });
  try {
    connection.deleteTopic(TOPIC);
    console.log(`[TEARDOWN] Topic '${TOPIC}' deleted successfully.`);
  } catch (error) {
    console.warn(`[TEARDOWN] Could not clean up topic: ${error}`);
  } finally {
    connection.close();
  }
}