import { check } from "k6";
import { 
    Writer, 
    Reader, 
    Connection, 
    SchemaRegistry, 
    CODEC_SNAPPY, 
    SCHEMA_TYPE_JSON 
} from "k6/x/kafka";

// Establish runtime parameters for the local broker environment
const brokers = ["localhost:9092"];
const topic = "test_json_pipeline";

// Initialize low-level transport and registry handlers
const connection = new Connection({ address: brokers[0] });
const schemaRegistry = new SchemaRegistry();

const writer = new Writer({
    brokers: brokers,
    topic: topic,
    autoCreateTopic: true,
});

// const reader = new Reader({
//     brokers: brokers,
//     topic: topic,
//     groupId: "xk6_json_consumer_group",
// });
const reader = new Reader({
    brokers: brokers,
    groupID: "xk6_json_consumer_group", 
    groupTopics: [topic], // Use groupTopics (as an array) instead of topic
});

export function setup() {
    // Explicitly create the topic during the setup phase
    connection.createTopic({ topic: topic, numPartitions: 1 });
}

export default function () {
    // RESOLUTION: Pass raw JavaScript objects to the serializer.
    // Do NOT wrap the payload in JSON.stringify().
    const messageKey = { correlationId: "txn-" + Math.floor(Math.random() * 100000) };
    const messageValue = { 
        templateId: 104, 
        status: "active", 
        timestamp: new Date().getTime() 
    };

    const messages = [
        {
            key: schemaRegistry.serialize({
                data: messageKey,
                schemaType: SCHEMA_TYPE_JSON,
            }),
            value: schemaRegistry.serialize({
                data: messageValue,
                schemaType: SCHEMA_TYPE_JSON,
            }),
        }
    ];

    // Dispatch the serialized payloads to the cluster
    writer.produce({ messages: messages });

    // Consume records back from the topic
    const consumed = reader.consume({ limit: 1 });

    if (consumed.length > 0) {
        const record = consumed[0];

        // Ensure key and value exist before deserialization
        const hasValidPayload = check(record, {
            "record key is populated": (r) => r.key !== null && r.key !== undefined,
            "record value is populated": (r) => r.value !== null && r.value !== undefined,
        });

        if (hasValidPayload) {
            // Deserialize passing the raw byte arrays back into the JSON engine
            const deserializedKey = schemaRegistry.deserialize({
                data: record.key,
                schemaType: SCHEMA_TYPE_JSON,
            });

            const deserializedValue = schemaRegistry.deserialize({
                data: record.value,
                schemaType: SCHEMA_TYPE_JSON,
            });

            check(deserializedValue, {
                "deserialized value is an object": (val) => typeof val === "object",
                "deserialized payload status matches": (val) => val.status === "active",
            });
        }
    }
}

export function teardown() {
    // Clean up active client sockets to prevent resource leaks
    writer.close();
    reader.close();
    connection.close();
}