"""Analyze and visualize test pipeline results."""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Dict, Any

def load_test_data(output_dir: Path) -> Dict[str, pd.DataFrame]:
    """
    Load test output data from CSV files.
    
    Args:
        output_dir: Directory containing test output files
        
    Returns:
        Dict of DataFrames containing test data
    """
    data = {}
    
    # Load summary
    summary_path = output_dir / 'summary.csv'
    if summary_path.exists():
        data['summary'] = pd.read_csv(summary_path)
        
    # Load memory usage
    memory_path = output_dir / 'memory_usage.csv'
    if memory_path.exists():
        data['memory'] = pd.read_csv(memory_path)
        data['memory']['timestamp'] = pd.to_datetime(data['memory']['timestamp'])
        
    # Load errors
    errors_path = output_dir / 'errors.csv'
    if errors_path.exists():
        data['errors'] = pd.read_csv(errors_path)
        data['errors']['timestamp'] = pd.to_datetime(data['errors']['timestamp'])
        
    return data

def plot_memory_usage(memory_df: pd.DataFrame, output_dir: Path):
    """Plot memory usage over time."""
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=memory_df, x='timestamp', y='memory_mb')
    plt.title('Memory Usage During Test Pipeline')
    plt.xlabel('Time')
    plt.ylabel('Memory Usage (MB)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_dir / 'memory_usage.png')
    plt.close()

def plot_event_distribution(summary_df: pd.DataFrame, output_dir: Path):
    """Plot distribution of event types."""
    # Extract event counts from summary
    events = pd.DataFrame([
        {'event_type': k, 'count': v}
        for k, v in eval(summary_df['events_by_type'].iloc[0]).items()
    ])
    
    plt.figure(figsize=(10, 6))
    sns.barplot(data=events, x='event_type', y='count')
    plt.title('Distribution of Event Types')
    plt.xlabel('Event Type')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_dir / 'event_distribution.png')
    plt.close()

def plot_timing_breakdown(summary_df: pd.DataFrame, output_dir: Path):
    """Plot timing breakdown of pipeline components."""
    timings = pd.DataFrame([{
        'component': 'GitHub API',
        'duration': summary_df['api_duration_seconds'].iloc[0]
    }, {
        'component': 'BigQuery',
        'duration': summary_df['bigquery_duration_seconds'].iloc[0]
    }, {
        'component': 'Other',
        'duration': (
            summary_df['duration_seconds'].iloc[0] -
            summary_df['api_duration_seconds'].iloc[0] -
            summary_df['bigquery_duration_seconds'].iloc[0]
        )
    }])
    
    plt.figure(figsize=(10, 6))
    sns.barplot(data=timings, x='component', y='duration')
    plt.title('Pipeline Timing Breakdown')
    plt.xlabel('Component')
    plt.ylabel('Duration (seconds)')
    plt.tight_layout()
    plt.savefig(output_dir / 'timing_breakdown.png')
    plt.close()

def generate_report(data: Dict[str, pd.DataFrame], output_dir: Path):
    """Generate analysis report in markdown format."""
    summary = data['summary'].iloc[0]
    
    report = f"""# Pipeline Test Analysis Report

## Overview
- **Total Duration**: {summary['duration_seconds']:.2f} seconds
- **Repositories Processed**: {summary['repositories_processed']}
- **Total Events**: {summary['total_events']}
- **Peak Memory Usage**: {summary['max_memory_mb']:.2f} MB
- **Errors**: {summary['error_count']}

## Timing Breakdown
- GitHub API: {summary['api_duration_seconds']:.2f} seconds
- BigQuery: {summary['bigquery_duration_seconds']:.2f} seconds
- Other Processing: {summary['duration_seconds'] - summary['api_duration_seconds'] - summary['bigquery_duration_seconds']:.2f} seconds

## Event Distribution
```
{pd.DataFrame([eval(summary['events_by_type'])]).T.to_string()}
```

## Memory Usage
- Peak: {summary['max_memory_mb']:.2f} MB
- See memory_usage.png for timeline

## Performance Metrics
- Events per Second: {summary['total_events'] / summary['duration_seconds']:.2f}
- MB per Repository: {summary['max_memory_mb'] / summary['repositories_processed']:.2f}

## Errors
"""
    
    if 'errors' in data:
        report += "\nDetailed errors found in errors.csv:\n```\n"
        report += data['errors'].to_string()
        report += "\n```"
    else:
        report += "\nNo errors recorded during test run."
        
    # Save report
    with open(output_dir / 'analysis_report.md', 'w') as f:
        f.write(report)

def main():
    """Analyze test results and generate visualizations."""
    output_dir = Path("tests/output")
    if not output_dir.exists():
        print(f"No test output found in {output_dir}")
        return
        
    # Load data
    data = load_test_data(output_dir)
    if not data:
        print("No test data found")
        return
        
    # Create visualizations
    if 'memory' in data:
        plot_memory_usage(data['memory'], output_dir)
        
    if 'summary' in data:
        plot_event_distribution(data['summary'], output_dir)
        plot_timing_breakdown(data['summary'], output_dir)
        
    # Generate report
    generate_report(data, output_dir)
    
    print(f"Analysis complete. Report saved to {output_dir}/analysis_report.md")

if __name__ == "__main__":
    main()
