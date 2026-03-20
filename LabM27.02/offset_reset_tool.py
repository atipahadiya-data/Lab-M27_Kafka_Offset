# offset_reset_tool.py
"""
StreamPulse Offset Reset Tool
Reset consumer group offsets for incident recovery.
"""
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
import argparse
import time

class OffsetResetTool:
    def __init__(self, bootstrap_servers):
        self.bootstrap = bootstrap_servers
        self.admin = AdminClient({'bootstrap.servers': bootstrap_servers})

    def get_current_offsets(self, group_id, topic, num_partitions):
        """Get current committed offsets for a consumer group."""
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap,
            'group.id': f'{group_id}-inspector',
            'enable.auto.commit': False,
        })

        tps = [TopicPartition(topic, p) for p in range(num_partitions)]
        committed = consumer.committed(tps, timeout=10)

        result = {}
        for tp in committed:
            lo, hi = consumer.get_watermark_offsets(tp, timeout=10)
            result[tp.partition] = {
                'committed': tp.offset if tp.offset >= 0 else 'none',
                'earliest': lo,
                'latest': hi,
                'lag': hi - tp.offset if tp.offset >= 0 else hi - lo,
            }

        consumer.close()
        return result

    def reset_to_earliest(self, group_id, topic, num_partitions):
        """Reset all offsets to the beginning."""
        # Implement: use admin API or consumer.seek + commit
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap,
            'group.id': group_id,
            'enable.auto.commit': False,
        })

        tps = [TopicPartition(topic, p) for p in range(num_partitions)]
        consumer.assign(tps)

        for tp in tps:
            lo, hi = consumer.get_watermark_offsets(tp, timeout=10)
            tp.offset = lo

        consumer.commit(offsets=tps, asynchronous=False)
        consumer.close()
        print(f'✅ Reset {group_id} to earliest for all {num_partitions} partitions')

    def reset_to_latest(self, group_id, topic, num_partitions):
        """Reset all offsets to the end (skip all existing events)."""
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap,
            'group.id': group_id,
            'enable.auto.commit': False,
        })

        tps = [TopicPartition(topic, p) for p in range(num_partitions)]
        consumer.assign(tps)

        for tp in tps:
            lo, hi = consumer.get_watermark_offsets(tp, timeout=10)
            tp.offset = hi

        consumer.commit(offsets=tps, asynchronous=False)
        consumer.close()
        print(f'✅ Reset {group_id} to latest for all {num_partitions} partitions')

    def reset_to_timestamp(self, group_id, topic, num_partitions, hours_ago):
        """Reset offsets to a specific timestamp."""
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap,
            'group.id': group_id,
            'enable.auto.commit': False,
        })

        target_ms = int((time.time() - hours_ago * 3600) * 1000)
        tps = [TopicPartition(topic, p, target_ms)
               for p in range(num_partitions)]

        offsets = consumer.offsets_for_times(tps, timeout=10)
        consumer.assign([TopicPartition(tp.topic, tp.partition)
                        for tp in offsets])
        consumer.commit(offsets=offsets, asynchronous=False)
        consumer.close()

        for tp in offsets:
            print(f'  P{tp.partition}: reset to offset {tp.offset}')
        print(f'✅ Reset {group_id} to {hours_ago}h ago')

# CLI
def main():
    parser = argparse.ArgumentParser(description='Offset Reset Tool')
    parser.add_argument('--group', required=True)
    parser.add_argument('--topic', required=True)
    parser.add_argument('--partitions', type=int, default=6)
    parser.add_argument('--action',
                       choices=['status', 'earliest', 'latest', 'timestamp'],
                       required=True)
    parser.add_argument('--hours-ago', type=float, default=1)

    args = parser.parse_args()
    tool = OffsetResetTool('localhost:9092')

    if args.action == 'status':
        offsets = tool.get_current_offsets(
            args.group, args.topic, args.partitions)

        print(f'\nGroup: {args.group}')
        print(f'Topic: {args.topic}')
        print(f'{"Part":>6} {"Committed":>12} {"Earliest":>12} '
              f'{"Latest":>12} {"Lag":>8}')
        print('-' * 56)

        total_lag = 0
        for p in sorted(offsets.keys()):
            o = offsets[p]
            total_lag += o['lag']
            print(f"{p:>6} {str(o['committed']):>12} {o['earliest']:>12} "
                  f"{o['latest']:>12} {o['lag']:>8}")
        print(f'{"TOTAL":>6} {"":>12} {"":>12} {"":>12} {total_lag:>8}')

    elif args.action == 'earliest':
        tool.reset_to_earliest(args.group, args.topic, args.partitions)

    elif args.action == 'latest':
        tool.reset_to_latest(args.group, args.topic, args.partitions)

    elif args.action == 'timestamp':
        tool.reset_to_timestamp(
            args.group, args.topic, args.partitions, args.hours_ago)

if __name__ == '__main__':
    main()
