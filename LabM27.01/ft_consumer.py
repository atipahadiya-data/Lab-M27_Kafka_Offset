# ft_consumer.py
"""
Fault-tolerant consumer with gap detection and duplicate tracking.
"""
from confluent_kafka import Consumer
import json
import time

CONFIG = {
    'bootstrap.servers': 'localhost:19092,localhost:19093,localhost:19094',
    'group.id': 'ft-verification-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'session.timeout.ms': 10000,
    'heartbeat.interval.ms': 3000,
}

class InstrumentedConsumer:
    def __init__(self):
        self.consumer = Consumer(CONFIG)
        self.consumer.subscribe(['ft-streampulse-events'])

        self.received_seqs = set()
        self.duplicate_count = 0
        self.error_count = 0
        self.total_consumed = 0
        self.start_time = time.time()

    def run(self):
        print('FT Consumer: reading events...')
        batch_count = 0

        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    self.error_count += 1
                    print(f'  Consumer error: {msg.error()}')
                    continue

                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    seq = event['seq']

                    if seq in self.received_seqs:
                        self.duplicate_count += 1
                    else:
                        self.received_seqs.add(seq)

                    self.total_consumed += 1
                    batch_count += 1

                except Exception as e:
                    self.error_count += 1

                if batch_count >= 100:
                    self.consumer.commit(asynchronous=False)
                    batch_count = 0

                if self.total_consumed % 500 == 0:
                    elapsed = time.time() - self.start_time
                    print(f'  [{elapsed:.0f}s] Consumed={self.total_consumed}, '
                          f'Unique={len(self.received_seqs)}, '
                          f'Duplicates={self.duplicate_count}')

        except KeyboardInterrupt:
            self.consumer.commit(asynchronous=False)
        finally:
            self.consumer.close()

    def report(self):
        print(f'\n{"="*50}')
        print(f'CONSUMER REPORT')
        print(f'  Total consumed:   {self.total_consumed}')
        print(f'  Unique events:    {len(self.received_seqs)}')
        print(f'  Duplicates:       {self.duplicate_count}')
        print(f'  Errors:           {self.error_count}')

        if self.received_seqs:
            max_seq = max(self.received_seqs)
            expected = set(range(max_seq + 1))
            missing = expected - self.received_seqs

            print(f'  Max sequence:     {max_seq}')
            print(f'  Missing events:   {len(missing)}')

            if missing:
                print(f'  ❌ GAPS DETECTED: {sorted(missing)[:20]}...')
            else:
                print(f'  ✅ ZERO GAPS — all events received!')

        print(f'{"="*50}')

if __name__ == '__main__':
    c = InstrumentedConsumer()
    try:
        c.run()
    except:
        pass
    finally:
        c.report()
