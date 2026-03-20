# lag_dashboard.py
"""
Multi-group consumer lag dashboard.
"""
from confluent_kafka import Consumer, TopicPartition
import time
import os

GROUPS = {
    'streampulse-analytics-v1': {
        'topic': 'streaming.user.interactions',
        'partitions': 6,
    },
    'streampulse-engagement-v1': {
        'topic': 'streaming.user.interactions',
        'partitions': 6,
    },
    'streampulse-revenue-v1': {
        'topic': 'payments.transaction.events',
        'partitions': 3,
    },
}

def check_group_lag(bootstrap, group_id, topic, num_partitions):
    """Check lag for a single consumer group."""
    consumer = Consumer({
        'bootstrap.servers': bootstrap,
        'group.id': f'{group_id}-dashboard',
        'enable.auto.commit': False,
    })

    tps = [TopicPartition(topic, p) for p in range(num_partitions)]

    try:
        committed = consumer.committed(tps, timeout=5)
    except:
        consumer.close()
        return None

    result = {'partitions': {}, 'total_lag': 0}

    for tp in committed:
        try:
            lo, hi = consumer.get_watermark_offsets(tp, timeout=5)
            offset = tp.offset if tp.offset >= 0 else lo
            lag = hi - offset
            result['partitions'][tp.partition] = {
                'committed': offset,
                'end': hi,
                'lag': lag,
            }
            result['total_lag'] += lag
        except:
            result['partitions'][tp.partition] = {
                'committed': '?', 'end': '?', 'lag': '?'
            }

    consumer.close()
    return result

def render_dashboard():
    """Render the lag dashboard."""
    os.system('clear')

    print(f'╔{"═"*60}╗')
    print(f'║  STREAMPULSE KAFKA LAG DASHBOARD  '
          f'{time.strftime("%H:%M:%S"):>25} ║')
    print(f'╠{"═"*60}╣')

    for group_id, info in GROUPS.items():
        result = check_group_lag(
            'localhost:9092',
            group_id,
            info['topic'],
            info['partitions']
        )

        if result is None:
            print(f'║  ❓ {group_id}: unable to fetch')
            continue

        lag = result['total_lag']
        status = '🟢' if lag < 100 else '🟡' if lag < 10000 else '🔴'

        print(f'║')
        print(f'║  {status} {group_id}')
        print(f'║     Topic: {info["topic"]}')
        print(f'║     Total lag: {lag:,}')

        # Per-partition bars
        for p in sorted(result['partitions'].keys()):
            pdata = result['partitions'][p]
            if isinstance(pdata['lag'], int):
                bar_len = min(int(pdata['lag'] / 100), 30)
                bar = '█' * bar_len
                print(f'║     P{p}: {pdata["lag"]:>8,} {bar}')

        print(f'║')

    print(f'╚{"═"*60}╝')
    print(f'\nRefreshing every 5 seconds... (Ctrl+C to stop)')

# Main loop
try:
    while True:
        render_dashboard()
        time.sleep(5)
except KeyboardInterrupt:
    print('\nDashboard stopped')
