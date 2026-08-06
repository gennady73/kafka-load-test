# Serverless Kafka Performance Framework for OpenShift

A decentralized, highly scalable, and entirely serverless performance testing framework for Apache Kafka, engineered natively for OpenShift and Kubernetes environments.

This framework utilizes a custom `k6` build (incorporating `xk6-kafka` and `xk6-dashboard`) distributed across ephemeral worker pods. It features a custom-built, Python-based Aggregation engine that processes raw worker metrics on-demand, generating both static HTML reports and live, interactive dashboard replays—all without requiring heavy persistent infrastructure like Prometheus or standalone Grafana instances.

---

## 🌟 Key Features

* **Air-Gap Ready:** Designed to be built externally and deployed completely offline in restricted enterprise environments.
* **Serverless Aggregation:** Smart Python aggregator that compiles reports on-demand, avoiding unnecessary CPU/RAM overhead.
* **Dual-Mode Observability:** Seamlessly switch between static HTML summaries (for archiving) and an interactive React web UI (for deep-dive root cause analysis).
* **Role-Based Orchestration:** Native support for simultaneous Producer and Consumer load testing with isolated metric tracking.
* **Zero Dependencies:** Bypasses strict OpenShift Security Context Constraints (SCC) via `/tmp` mounts and operates entirely within its own namespace.

---

## Phase 1: Online Preparation (Pre-Air-Gap)

Before deploying inside the isolated network, you need the required container images available in your internal registry. You can either **transfer pre-built container images** or **build them from source**.

### Option A: Use Pre-Built Images (Fast-Track)

If pre-built images are already available in an external registry or provided as tarball archives, you can **completely skip the build process**.

1. **Pull the Existing Images:**
   ```bash
   docker pull <external-registry>/perf/k6-ubi:latest
   docker pull <external-registry>/perf/k6-web-dashboard:latest
   docker pull <external-registry>/perf/k6-aggregator:latest
   ```

2. Transfer to Air-Gapped Registry:
Export them to tar archives (if transferring via physical media) or re-tag and push directly to your internal corporate registry:
    ```bash
    # Save as tar archives for offline transfer
    docker save <external-registry>/perf/k6-web-dashboard:latest -o k6-web-dashboard.tar

    # On the air-gapped side: Load, tag, and push to internal registry
    docker load -i k6-web-dashboard.tar
    docker tag <external-registry>/perf/k6-web-dashboard:latest [your-corporate-registry.com/perf/k6-web-dashboard:latest](https://your-corporate-registry.com/perf/k6-web-dashboard:latest)
    docker push [your-corporate-registry.com/perf/k6-web-dashboard:latest](https://your-corporate-registry.com/perf/k6-web-dashboard:latest)
    ```

### Option B: Build from Source (Custom Compilation)
If you need to build the custom binaries and container images from scratch on an internet-connected workstation:

1. Build the Custom `k6` Binary:

    ```bash
    go install go.k6.io/xk6/cmd/xk6@latest

    xk6 build \
    --with [github.com/mostafa/xk6-kafka](https://github.com/mostafa/xk6-kafka) \
    --with [github.com/grafana/xk6-dashboard](https://github.com/grafana/xk6-dashboard)
    ```

2. Package the Container Images:
    ```bash
    # Build the base UBI image
    docker build -t [your-corporate-registry.com/perf/k6-ubi:latest](https://your-corporate-registry.com/perf/k6-ubi:latest) -f docker/Dockerfile-ubi .

    # Build the Web Dashboard enabled image
    docker build -t [your-corporate-registry.com/perf/k6-web-dashboard:latest](https://your-corporate-registry.com/perf/k6-web-dashboard:latest) -f docker/Dockerfile-ubi-k6-web-dashboard .

    # Build the Aggregator image
    docker build -t [your-corporate-registry.com/perf/k6-aggregator:latest](https://your-corporate-registry.com/perf/k6-aggregator:latest) -f docker/Dockerfile-ubi-k6-aggregator .
    ```

3. Push to Internal Registry:

    ```bash
    docker push [your-corporate-registry.com/perf/k6-ubi:latest](https://your-corporate-registry.com/perf/k6-ubi:latest)
    docker push [your-corporate-registry.com/perf/k6-web-dashboard:latest](https://your-corporate-registry.com/perf/k6-web-dashboard:latest)
    docker push [your-corporate-registry.com/perf/k6-aggregator:latest](https://your-corporate-registry.com/perf/k6-aggregator:latest)
    ```

## Phase 2: Air-Gapped Execution
Once inside the isolated environment, ensure all Kubernetes manifests (`k8s/workers/*.yaml`) have their `image`: properties updated to point to your internal registry.

1. Environment Setup
Create the namespace and deploy the Aggregator pipeline:
    ```bash
    # Create namespace and apply configurations
    oc apply -f k8s/setup/k6-namespace.yaml
    oc apply -f k8s/setup/k6-configmap.yaml
    oc apply -f k8s/observability/aggregator/k6-aggregator-configmap.yaml

    # Deploy the Aggregator and Expose Routes
    oc apply -f k8s/observability/aggregator/k6-aggregator-deployment.yaml
    oc apply -f k8s/observability/aggregator/k6-aggregator-service.yaml
    oc apply -f k8s/observability/aggregator/k6-routes.yaml
    ```

2. Access the Control Panel
Retrieve your generated routes:
    ```bash
    oc get routes -n performance-testing -l app=k6-aggregator
    ```
    Open the `k6-control-panel` URL in your browser. You will use this interface to track test data, clear the environment, and trigger reports.

3. Execute the Load Test
Trigger your worker pods. You can run Producers and Consumers simultaneously or sequentially:
    ```bash
    oc apply -f k8s/workers/k6-producer-job-metrics-web.yaml
    oc apply -f k8s/workers/k6-consumer-job-metrics-web.yaml
    ```

4. View Results
    1. Navigate to your `k6-control-panel` URL.

    2. Click `/start-replay` to trigger the on-demand aggregation engine.

    3. Once the terminal UI indicates SUCCESS, open the `k6-replay-server` URL in a new tab to view your interactive Kafka dashboard.

    4. (Optional) Click `/reset` on the Control Panel to wipe the data directory before starting a new test iteration.

---

### Script Configuration & Telemetry
The core load generation script (`tests/kafka-perf-framework.js`) is designed to be environment-agnostic. All Kafka connection details, credentials, and telemetry outputs are controlled dynamically via the OpenShift Job manifests (`k8s/workers/*.yaml`), meaning the JavaScript code never needs to be modified between environments.

1. Environment Variables (Kafka Configuration)
Inside your Job manifests, configure the test parameters using standard Kubernetes `env` blocks:

- `KAFKA_BROKERS`: Comma-separated list of bootstrap servers.

- `KAFKA_TOPIC`: The target topic for the Producer/Consumer.

- `PRODUCER_SCALING` / `CONSUMER_SCALING`: Adjust VU (Virtual User) counts and throughput limits.

2. Telemetry Output Modes

    **Mode A: Headless / Console-Only (Zero Telemetry Overhead)**

    * **Best For:** Ultra-high-throughput testing, long-running soak tests, or quick validation runs where you want zero performance degradation caused by metric export disk I/O or network traffic.

    * **How It Works:** Each `k6` worker pod runs completely isolated. Metrics are printed directly to `stdout`.

        ```yaml
        # k8s/workers/k6-producer-headless-job.yaml
        args: ["-c", "k6 run /script/kafka-perf-framework.js"]
        ```
        
        Retrieve statistics directly from the pod logs:
        
        ```bash
        oc logs job/k6-producer-headless -n performance-testing
        ```

    **Mode B: Serverless Aggregator (Short-to-Medium Tests)**

    * **Best For:** Deep-dive debugging, isolated testing, and rich, interactive HTML reports without spinning up persistent monitoring infrastructure.

    * **Configuration:** Output to JSON and POST the data to the custom Python Aggregator.

        ```yaml
        # k8s/workers/k6-producer-job-metrics-web.yaml
        args: 
        - "-c"
        - >
            k6 run --out 'json=/tmp/raw-metrics.json' /script/kafka-perf-framework.js && 
            curl -X POST --data-binary @/tmp/raw-metrics.json '[http://k6-aggregator.performance-testing.svc:8080/upload/$](http://k6-aggregator.performance-testing.svc:8080/upload/$){POD_NAME}.json?role=producer&total=${EXPECTED_PODS}'
        ```

    **Mode C: Prometheus / OpenTelemetry (Long-Running / Massive Tests)**
    
    * **Best For:** Multi-hour endurance testing, soak tests, or massive horizontal scaling where JSON aggregation would overwhelm memory limits.

    * **Configuration:** Stream metrics directly to OpenTelemetry or Prometheus Remote Write targets.

        ```yaml
        # k8s/workers/k6-producer-job-metrics-otel.yaml
        env:
        - name: K6_PROMETHEUS_RW_SERVER_URL
            value: "[http://prometheus-operated.monitoring.svc:9090/api/v1/write](http://prometheus-operated.monitoring.svc:9090/api/v1/write)"
        args: 
        - "-c"
        - "k6 run --out experimental-prometheus-rw /script/kafka-perf-framework.js"
        ```

    **Mode D: Scheduled Jobs (Automated / Recurring Workloads)**

    * **Best For:** Nightly performance regressions, periodic cluster baseline benchmarks, or automated soak testing scheduled during off-peak hours.

        ```yaml
        # k8s/workers/k6-producer-sched-job-metrics-web.yaml
        apiVersion: batch/v1
        kind: CronJob
        metadata:
        name: k6-kafka-scheduled-test
        namespace: performance-testing
        spec:
        schedule: "0 2 * * *" # Runs daily at 02:00 AM
        jobTemplate:
            spec:
            template:
                spec:
                restartPolicy: Never
                containers:
                    - name: k6-worker
                    image: [your-corporate-registry.com/perf/k6-ubi:latest](https://your-corporate-registry.com/perf/k6-ubi:latest)
                    command: ["/bin/sh"]
                    args: ["-c", "k6 run /script/kafka-perf-framework.js"]
        ```

---
