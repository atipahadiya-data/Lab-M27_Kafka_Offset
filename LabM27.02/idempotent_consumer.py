# idempotent_consumer.py
"""
Consumer with idempotent processing — safe for at-least-once delivery.
Uses a local dedup store to detect and skip duplicate events.
"""
from confluent_kafka import Consumer
import json
import time
import sqlite3

class IdempotentConsumer:
    def __init__(self, db_path='processed_events.db'):
        # SQLite for dedup tracking
        self.db = sqlite3.connect(db_path)
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS processed (
                event_id TEXT PRIMARY KEY,
                processed_at REAL,
                partition INTEGER,
                offset_val INTEGER
            )
        ''')
        self.db.commit()

        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'idempotent-consumer-v1',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        self.consumer.subscribe(['streaming.user.interactions'])

        self.stats = {
            'processed': 0,
            'duplicates_skipped': 0,
            'errors': 0,
        }

    def is_duplicate(self, event_id):
        """Check if event was already processed."""
        cursor = self.db.execute(
            'SELECT 1 FROM processed WHERE event_id = ?',
            (event_id,)
        )
        return cursor.fetchone() is not None

    def mark_processed(self, event_id, partition, offset):
        """Record that event was processed."""
        self.db.execute(
            'INSERT OR IGNORE INTO processed VALUES (?, ?, ?, ?)',
            (event_id, time.time(), partition, offset)
        )

    def process_event(self, event, partition, offset):
        """Process a single event idempotently."""
        event_id = event.get('event_id')
        if not event_id:
            event_id = f'{partition}-{offset}'

        if self.is_duplicate(event_id):
            self.stats['duplicates_skipped'] += 1
            return True  # Already processed

        # YOUR BUSINESS LOGIC HERE
        # Example: write to database, update aggregation, etc.

        self.mark_processed(event_id, partition, offset)
        self.stats['processed'] += 1
        return True

    def run(self):
        batch_count = 0

        try:
            print('Idempotent Consumer running...')

            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    continue

                event = json.loads(msg.value().decode('utf-8'))
                self.process_event(event, msg.partition(), msg.offset())
                batch_count += 1

                if batch_count >= 100:
                    self.db.commit()  # Batch DB writes
                    self.consumer.commit(asynchronous=False)
                    batch_count = 0

                total = self.stats['processed'] + self.stats['duplicates_skipped']
                if total % 500 == 0:
                    print(f"  Processed: {self.stats['processed']}, "
                          f"Duplicates skipped: {self.stats['duplicates_skipped']}")

        except KeyboardInterrupt:
            self.db.commit()
            self.consumer.commit(asynchronous=False)

            print(f"\nFinal stats:")
            print(f"  Processed: {self.stats['processed']}")
            print(f"  Duplicates skipped: {self.stats['duplicates_skipped']}")
            print(f"  Errors: {self.stats['errors']}")

        finally:
            self.consumer.close()
            self.db.close()

if __name__ == '__main__':
    consumer = IdempotentConsumer()
    consumer.run()
