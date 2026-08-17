import { check, sleep } from "k6";
    
// AIR-GAP SAFE: Import locally from the ConfigMap volume
import { uuidv4 } from "/script/k6-utils.js"; 

import { 
    Producer, 
    Consumer, 
    AdminClient, 
    SchemaRegistry,
    SCHEMA_TYPE_STRING,
    SCHEMA_TYPE_JSON,
    SCHEMA_TYPE_AVRO,
    SCHEMA_TYPE_PROTOBUF
} from "k6/x/kafka";

// =============================================================================
// 1. FRAMEWORK CONFIGURATION 
// =============================================================================
const CONFIG = {
    // Infrastructure
    brokers: (__ENV.KAFKA_BROKERS || "localhost:9092").split(","),
    topic: __ENV.KAFKA_TOPIC || "performance_evaluation_pipeline",
    registryUrl: __ENV.SCHEMA_REGISTRY_URL || "http://localhost:8081",
    
    // Execution Profile
    role: __ENV.TEST_ROLE || "PRODUCER",               // PRODUCER, CONSUMER
    payloadSize: __ENV.PAYLOAD_SIZE || "1k",           // e.g., 256b, 1k, 1m, 5m
    serialization: __ENV.SERIALIZATION_TYPE || "JSON", // STRING, JSON, AVRO, PROTOBUF
    compression: __ENV.COMPRESSION_TYPE || "SNAPPY",   // SNAPPY, ZSTD, GZIP, NONE
    
    // Load Dial
    targetRps: parseInt(__ENV.TARGET_RPS || "5000", 10),
    duration: __ENV.TEST_DURATION || "5m",
    maxVus: parseInt(__ENV.MAX_VUS || "100", 10),

    // Advanced Kafka Limits
    clientMaxBytes: parseInt(__ENV.CLIENT_MAX_BYTES || "104857600", 10), 
    topicMaxBytes: __ENV.TOPIC_MAX_BYTES || "10485760", 
    batchMessages: parseInt(__ENV.MAX_BATCH_MESSAGES || "65536", 10),
    batchBytes: parseInt(__ENV.MAX_BATCH_BYTES || "52428800", 10), // 50MB
};

// =============================================================================
// 2. PRE-LOADED PAYLOAD STORAGE
// =============================================================================
const payloadPath = `/mnt/payloads/message-${CONFIG.payloadSize}.json`;
let SELECTED_PAYLOAD_STRING;

try {
    SELECTED_PAYLOAD_STRING = open(payloadPath);
    console.log(`[INIT] Successfully loaded payload from: ${payloadPath}`);
} catch (error) {
    console.error(`[FATAL] Could not read payload file at ${payloadPath}`);
    throw error;
}

// =============================================================================
// 3. K6 SCENARIO RUNTIME DESIGN
// =============================================================================
export const options = {
    scenarios: {
        throughput_test: {
            executor: "constant-arrival-rate",
            rate: CONFIG.targetRps,
            timeUnit: "1s",
            duration: CONFIG.duration,
            preAllocatedVUs: Math.min(50, CONFIG.maxVus),
            maxVUs: CONFIG.maxVus,
        },
    },
    thresholds: {
        kafka_writer_error_count: ["count == 0"],
        kafka_reader_error_count: ["count == 0"],
    }
};

// =============================================================================
// 4. CLIENT INITIALIZATION & SCHEMA REGISTRY
// =============================================================================
const adminClient = new AdminClient({ brokers: CONFIG.brokers });
const schemaRegistry = new SchemaRegistry({ url: CONFIG.registryUrl });

let producer = null;
let consumer = null;

if (CONFIG.role === "PRODUCER") {
    producer = new Producer({
        brokers: CONFIG.brokers,
        compression: CONFIG.compression === "NONE" ? null : CONFIG.compression.toLowerCase(),
        batchSize: CONFIG.batchMessages, 
        batchBytes: CONFIG.batchBytes, 
        extraConfig: {
            "message.max.bytes": String(CONFIG.clientMaxBytes), 
        },
    });
} else {
    consumer = new Consumer({
        brokers: CONFIG.brokers,
        topic: CONFIG.topic,
        groupId: `xk6_perf_group_${CONFIG.serialization.toLowerCase()}_${uuidv4().substring(0, 8)}`, 
        maxBytes: CONFIG.clientMaxBytes, // Natively maps to fetch.max.bytes
        extraConfig: {
            "message.max.bytes": String(CONFIG.clientMaxBytes),
        },
    });
}

// Ensure the target Schema exists or retrieve it
let valueSchemaObject = null;
if (CONFIG.serialization === "AVRO" || CONFIG.serialization === "PROTOBUF") {
    try {
        valueSchemaObject = schemaRegistry.getSchema({ 
            subject: `${CONFIG.topic}-value` 
        });
    } catch (e) {
        if (CONFIG.serialization === "AVRO") {
            const avroSchema = `{"type":"record","name":"Event","namespace":"perf","fields":[{"name":"id","type":"string"},{"name":"timestamp","type":"long"},{"name":"payload","type":"string"}]}`;
            valueSchemaObject = schemaRegistry.createSchema({
                subject: `${CONFIG.topic}-value`,
                schema: avroSchema,
                schemaType: SCHEMA_TYPE_AVRO
            });
        } else if (CONFIG.serialization === "PROTOBUF") {
            const protoSchema = `
            syntax = "proto3";
            package perf;
            message Event {
            string id = 1;
            int64 timestamp = 2;
            string payload = 3;
            }`;
            valueSchemaObject = schemaRegistry.createSchema({
                subject: `${CONFIG.topic}-value`,
                schema: protoSchema,
                schemaType: SCHEMA_TYPE_PROTOBUF
            });
        }
    }
}

// =============================================================================
// 5. SETUP LIFECYCLE (Executes once before load runs)
// =============================================================================
export function setup() {
    console.log(`[SETUP] Ensuring topic '${CONFIG.topic}' exists...`);
    
    const currentTopics = adminClient.listTopics();
    const topicExists = currentTopics.some(t => t.topic === CONFIG.topic || t.name === CONFIG.topic);

    if (!topicExists) {
        console.log(`[SETUP] Topic '${CONFIG.topic}' not found. Creating it now...`);
        try {
            adminClient.createTopic({ 
                topic: CONFIG.topic, 
                numPartitions: 12, 
                replicationFactor: 1,
                configEntries: [
                    {
                        configName: "max.message.bytes",
                        configValue: String(CONFIG.topicMaxBytes)
                    }
                ]
            });
            sleep(2); 
            console.log(`[SETUP] Topic '${CONFIG.topic}' created successfully.`);
        } catch (error) {
            if (!error.message.includes("already exists")) throw error;
            console.log(`[SETUP] Topic '${CONFIG.topic}' was created concurrently.`);
        }
    } else {
        console.log(`[SETUP] Topic '${CONFIG.topic}' already exists.`);
    }
}

// =============================================================================
// 6. CORE LOAD ITERATION
// =============================================================================
export default function () {
    const correlationId = uuidv4();
    const timestamp = new Date().getTime();

    if (CONFIG.role === "PRODUCER") {
        let serializedKey;
        let serializedValue;

        const messageKey = { correlationId: correlationId };
        const messageValue = { 
            id: correlationId, 
            timestamp: timestamp, 
            payload: SELECTED_PAYLOAD_STRING
        };

        if (CONFIG.serialization === "JSON") {
            serializedKey = schemaRegistry.serialize({ data: messageKey, schemaType: SCHEMA_TYPE_JSON });
            serializedValue = schemaRegistry.serialize({ data: messageValue, schemaType: SCHEMA_TYPE_JSON });
        } else if (CONFIG.serialization === "AVRO") {
            serializedKey = schemaRegistry.serialize({ data: correlationId, schemaType: SCHEMA_TYPE_STRING });
            serializedValue = schemaRegistry.serialize({ data: messageValue, schema: valueSchemaObject, schemaType: SCHEMA_TYPE_AVRO });
        } else if (CONFIG.serialization === "PROTOBUF") {
            serializedKey = schemaRegistry.serialize({ data: correlationId, schemaType: SCHEMA_TYPE_STRING });
            serializedValue = schemaRegistry.serialize({ data: messageValue, schema: valueSchemaObject, schemaType: SCHEMA_TYPE_PROTOBUF });
        } else {
            serializedKey = schemaRegistry.serialize({ data: correlationId, schemaType: SCHEMA_TYPE_STRING });
            serializedValue = schemaRegistry.serialize({ data: SELECTED_PAYLOAD_STRING, schemaType: SCHEMA_TYPE_STRING });
        }

        producer.produce({
            messages: [{ topic: CONFIG.topic, key: serializedKey, value: serializedValue }]
        });

    } else {
        const consumed = consumer.consume({ maxMessages: 100, expectTimeout: true });
        check(consumed, {
            "Consumer fetched messages successfully": (msg) => msg.length > 0
        });
    }
}

// =============================================================================
// 7. TEARDOWN LIFECYCLE
// =============================================================================
export function teardown() {
    console.log("[TEARDOWN] Closing connection handles...");
    if (producer) producer.close();
    if (consumer) consumer.close();
    adminClient.close();
}