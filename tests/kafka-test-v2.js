import { check, sleep } from "k6";
import { Producer, Consumer, AdminClient, SchemaRegistry, SCHEMA_TYPE_JSON } from "k6/x/kafka";

// 1. Connection settings derived from environment variables or default to local development
const BROKERS = [__ENV.KAFKA_BROKERS || "localhost:9092"];
const TOPIC = "xk6-performance-benchmark";

// 2. Initialize the Schema Registry client (useful for Avro, Protobuf, or JSON schema validation)
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
    // Fail the test if any errors occur during execution
    kafka_writer_error_count: ["count == 0"],
    kafka_reader_error_count: ["count == 0"],
  },
};

// 4. SETUP PHASE: Runs ONCE before VUs start.
// Best practice: Create and verify the topic here to avoid VU race conditions [cite: 1, 3].
export function setup() {
  const admin = new AdminClient({ brokers: BROKERS });
  
  console.log(`[SETUP] Ensuring topic '${TOPIC}' exists...`);
  admin.createTopic({
    topic: TOPIC,
    numPartitions: 3,
    replicationFactor: 1,
  });

  admin.close();
  
  // Wait briefly for partition and metadata propagation across the cluster
  sleep(2);
}

// 5. VU ITERATION PHASE: This is the code executed in a loop by each Virtual User (VU).
export default function () {
  const producer = new Producer({ brokers: BROKERS, topic: TOPIC });
  const consumer = new Consumer({ brokers: BROKERS, topic: TOPIC, groupId: "k6-load-test-group" });

  const messageKey = { id: `key-${__VU}-${__ITER}` };
  const messageValue = {
    payload: "Performance Testing Serialization",
    timestamp: Date.now(),
    virtualUser: __VU,
    iteration: __ITER,
  };

  // Produce message to Kafka
  producer.produce({
    messages: [
      {
        key: schemaRegistry.serialize({
          data: JSON.stringify(messageKey),
          schemaType: SCHEMA_TYPE_JSON,
        }),
        value: schemaRegistry.serialize({
          data: JSON.stringify(messageValue),
          schemaType: SCHEMA_TYPE_JSON,
        }),
      },
    ],
  });

  // Consume message from Kafka
  const receivedMessages = consumer.consume({ maxMessages: 1 });

  // Validate that message was written and read successfully [cite: 4]
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

  // Clean up connections for the VU iteration to prevent resource leaks
  producer.close();
  consumer.close();

  // Pacing: Wait 1 second between iterations
  sleep(1);
}

// 6. TEARDOWN PHASE: Runs ONCE at the end of the test.
export function teardown() {
  console.log("[TEARDOWN] Test complete. Cleaning up cluster topics...");
  const admin = new AdminClient({ brokers: BROKERS });
  try {
    admin.deleteTopic(TOPIC);
    console.log(`[TEARDOWN] Topic '${TOPIC}' deleted successfully.`);
  } catch (error) {
    console.warn(`[TEARDOWN] Could not clean up topic: ${error}`);
  } finally {
    admin.close();
  }
}