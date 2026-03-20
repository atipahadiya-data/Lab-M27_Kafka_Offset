"""
Run all verification checks and produce a summary report.
"""

def generate_report(producer_metrics, consumer_metrics):
    """Generate the final fault tolerance report."""

    # Handle long lists for readability
    def format_list(lst, max_len=10):
        if not lst:
            return "[]"
        if len(lst) > max_len:
            return str(lst[:max_len]) + "..."
        return str(lst)

    producer_failures_str = format_list(producer_metrics.get('failed_sequences', []))
    consumer_gaps_str = format_list(consumer_metrics.get('gaps', []))

    report = f"""
╔══════════════════════════════════════════════════════╗
║  STREAMPULSE KAFKA FAULT TOLERANCE REPORT           ║
╠══════════════════════════════════════════════════════╣
║                                                     ║
║  PRODUCER                                           ║
║  ├── Events produced:   {producer_metrics['total_produced']:<10}               ║
║  ├── Events delivered:  {producer_metrics['total_delivered']:<10}               ║
║  ├── Events failed:     {producer_metrics['total_failed']:<10}               ║
║  ├── Delivery rate:     {producer_metrics['total_delivered']/max(producer_metrics['total_produced'],1)*100:.1f}%                    ║
║  └── Failed sequences:  {producer_failures_str:<50}       ║
║                                                     ║
║  CONSUMER                                           ║
║  ├── Events consumed:   {consumer_metrics['total']:<10}               ║
║  ├── Unique events:     {consumer_metrics['unique']:<10}               ║
║  ├── Duplicates:        {consumer_metrics['duplicates']:<10}               ║
║  ├── Missing (gaps):    {consumer_metrics['missing']:<10} {consumer_gaps_str:<50} ║
║  ├── Errors:            {consumer_metrics['errors']:<10}               ║
║                                                     ║
║  VERDICT                                            ║
║  ├── Data loss:         {'❌ YES' if consumer_metrics['missing'] > 0 else '✅ NONE':<15}            ║
║  ├── Duplicates:        {'⚠️  YES' if consumer_metrics['duplicates'] > 0 else '✅ NONE':<15}            ║
║  └── Overall:           {'✅ PASS' if consumer_metrics['missing'] == 0 else '❌ FAIL':<15}            ║
║                                                     ║
╚══════════════════════════════════════════════════════╝
"""
    print(report)


if __name__ == "__main__":
    # Example metrics, replace with actual collected data
    producer_metrics = {
        'total_produced': 3000,
        'total_delivered': 2910,
        'total_failed': 90,
        'failed_sequences': [1760, 1761, 1766, 1767, 1762, 1763, 1768, 1769, 1764, 1765, 1772, 1773, 1778, 1779, 1774, 1775, 1770, 1771, 1776, 1777]
    }

    consumer_metrics = {
        'total': 1220,
        'unique': 1210,
        'duplicates': 10,
        'errors': 0,
        'missing': 1790,
        'gaps': list(range(0, 20))  # sample gaps for readability
    }

    generate_report(producer_metrics, consumer_metrics)