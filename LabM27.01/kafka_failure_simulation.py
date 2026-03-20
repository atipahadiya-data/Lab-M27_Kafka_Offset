# kafka_failure_simulation.py

import docker
import subprocess
import time
from datetime import datetime

# ===============================
# CONFIGURATION
# ===============================
BROKERS = ["kafka-1", "kafka-2", "kafka-3"]
TOPIC = "ft-streampulse-events"
WAIT_TIME = 30  # seconds to wait during failure
LOG_FILE = "failure_simulation.log"

client = docker.from_env()


# ===============================
# LOGGING HELPER
# ===============================
def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")
    with open(LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] {msg}\n")


# ===============================
# DOCKER HELPER FUNCTIONS
# ===============================
def stop_container(name):
    log(f"Stopping container {name}...")
    client.containers.get(name).stop()
    log(f"{name} stopped")


def start_container(name):
    log(f"Starting container {name}...")
    client.containers.get(name).start()
    log(f"{name} started")


def restart_container(name):
    log(f"Restarting container {name}...")
    client.containers.get(name).restart()
    log(f"{name} restarted")


# ===============================
# KAFKA HELPER FUNCTIONS
# ===============================
def get_partition_leader(topic, partition=0):
    """Return broker ID of leader for given partition"""
    cmd = [
        "docker", "exec", "kafka-1",
        "kafka-topics",
        "--describe",
        "--bootstrap-server", "localhost:9092",
        "--topic", topic
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    for line in result.stdout.splitlines():
        if f"Partition: {partition}" in line:
            parts = line.split()
            for i, p in enumerate(parts):
                if p == "Leader:":
                    return parts[i + 1]
    return None


# ===============================
# SCENARIOS
# ===============================
def scenario_single_follower_crash():
    """Stop one follower and bring back"""
    log("=== Scenario 1: Single Follower Crash ===")
    stop_container("kafka-3")
    log(f"Waiting {WAIT_TIME} seconds while follower is down...")
    time.sleep(WAIT_TIME)
    start_container("kafka-3")
    log("Scenario 1 complete\n")


def scenario_leader_crash():
    """Stop leader for partition 0"""
    log("=== Scenario 2: Leader Crash ===")
    leader = get_partition_leader(TOPIC, partition=0)
    if not leader:
        log("Cannot determine leader!")
        return
    log(f"Leader for partition 0 is kafka-{leader}")
    stop_container(f"kafka-{leader}")
    log(f"Waiting {WAIT_TIME} seconds for leader election...")
    time.sleep(WAIT_TIME)
    start_container(f"kafka-{leader}")
    log("Scenario 2 complete\n")


def scenario_two_brokers_down():
    """Stop 2 brokers to trigger NotEnoughReplicasException"""
    log("=== Scenario 3: Two Brokers Down ===")
    stop_container("kafka-2")
    stop_container("kafka-3")
    log(f"Waiting {WAIT_TIME} seconds while 2 brokers are down...")
    time.sleep(WAIT_TIME)
    start_container("kafka-2")
    log(f"Waiting {WAIT_TIME} seconds for writes to resume...")
    time.sleep(WAIT_TIME)
    start_container("kafka-3")
    log("Scenario 3 complete\n")


def scenario_consumer_hard_kill():
    """Hard kill consumer process"""
    log("=== Scenario 4: Consumer Hard Kill ===")
    # Find ft_consumer PID
    result = subprocess.run(
        ["pgrep", "-f", "ft_consumer.py"], capture_output=True, text=True
    )
    pids = result.stdout.strip().splitlines()
    if not pids:
        log("No consumer process found!")
        return
    for pid in pids:
        log(f"Killing consumer PID {pid}")
        subprocess.run(["kill", "-9", pid])
    log(f"Waiting {WAIT_TIME} seconds for rebalance...")
    time.sleep(WAIT_TIME)
    log("Scenario 4 complete\n")


def scenario_rolling_restart():
    """Restart brokers one at a time"""
    log("=== Scenario 5: Rolling Broker Restart ===")
    for broker in BROKERS:
        restart_container(broker)
        log(f"Waiting {WAIT_TIME} seconds after restarting {broker}...")
        time.sleep(WAIT_TIME)
    log("Scenario 5 complete\n")


# ===============================
# MAIN TEST SUITE
# ===============================
if __name__ == "__main__":
    log("=== Starting Kafka Failure Simulation ===\n")

    scenario_single_follower_crash()
    scenario_leader_crash()
    scenario_two_brokers_down()
    scenario_consumer_hard_kill()
    scenario_rolling_restart()

    log("=== Kafka Failure Simulation Completed ===")