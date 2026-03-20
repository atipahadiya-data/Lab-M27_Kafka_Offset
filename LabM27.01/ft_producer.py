# ft_producer.py
"""
Fault-tolerant producer with sequence numbers and delivery tracking.
"""
from confluent_kafka import Producer
import json
import time
import threading

CONFIG = {
    'bootstrap.servers': 'localhost:19092,localhost:19093,localhost:19094',
    'acks': 'all',
    'enable.idempotence': True,
    'retries': 10,
    'retry.backoff.ms': 500,
    'delivery.timeout.ms': 60000,
    'request.timeout.ms': 10000,
    'linger.ms': 10,
    'compression.type': 'snappy',
}

class InstrumentedProducer:
    def __init__(self):
        self.producer = Producer(CONFIG)
        self.lock = threading.Lock()
        self.metrics = {
            'total_produced': 0,
            'total_delivered': 0,
            'total_failed': 0,
            'failed_sequences': [],
            'delivery_latencies': [],
        }

    def _callback(self, err, msg):
        with self.lock:
            if err:
                self.metrics['total_failed'] += 1
                seq = json.loads(msg.value())['seq']
                self.metrics['failed_sequences'].append(seq)
                print(f'  ❌ FAILED seq: {seq} — {err}')
            else:
                self.metrics['total_delivered'] += 1

    def produce_event(self, seq):
        event = {
            'seq': seq,
            'timestamp': time.time(),
            'producer_id': 'ft-producer-1',
        }

        try:
            self.producer.produce(
                topic='ft-streampulse-events',
                key=str(seq % 6),
                value=json.dumps(event),
                callback=self._callback,
            )
            self.metrics['total_produced'] += 1
        except BufferError:
            self.producer.flush()
            self.producer.produce(
                topic='ft-streampulse-events',
                key=str(seq % 6),
                value=json.dumps(event),
                callback=self._callback,
            )
            self.metrics['total_produced'] += 1

        self.producer.poll(0)

    def run(self, rate=10, duration=300):
        """Run producer at given rate for given duration."""
        print(f'FT Producer: {rate} events/sec for {duration}s')
        print(f'Config: acks=all, idempotent=true, retries=10\n')

        start = time.time()
        seq = 0

        while time.time() - start < duration:
            batch_start = time.time()

            for _ in range(rate):
                self.produce_event(seq)
                seq += 1

            elapsed = time.time() - batch_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)

            # Report every 10 seconds
            if int(time.time() - start) % 10 == 0 and seq % rate == 0:
                print(f'  [{int(time.time()-start)}s] Produced={self.metrics["total_produced"]}, '
                      f'Delivered={self.metrics["total_delivered"]}, '
                      f'Failed={self.metrics["total_failed"]}')

        self.producer.flush()
        return self.metrics

    def report(self):
        m = self.metrics
        print(f'\n{"="*50}')
        print(f'PRODUCER REPORT')
        print(f'  Total produced:  {m["total_produced"]}')
        print(f'  Total delivered: {m["total_delivered"]}')
        print(f'  Total failed:    {m["total_failed"]}')
        print(f'  Failed sequences: {m["failed_sequences"][:20]}')
        print(f'  Delivery rate:   {m["total_delivered"]/m["total_produced"]*100:.2f}%')
        print(f'{"="*50}')

if __name__ == '__main__':
    p = InstrumentedProducer()
    try:
        p.run(rate=10, duration=300)
    except KeyboardInterrupt:
        p.producer.flush()
    finally:
        p.report()
