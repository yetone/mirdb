/**
 * Performance Metrics Section Component
 * Owner: Scenario 5 - Performance Metrics Display
 *
 * Displays benchmark comparisons:
 * - Throughput (ops/sec)
 * - Latency (ms)
 * - Memory usage (MB)
 * - Comparison with at least one other database
 */

import { performanceMetrics, databaseComparisons } from '../../data/performance';
import { PerformanceMetric } from '../../types';
import './Performance.css';

/**
 * Icon components for metrics
 */
const MetricIcon = ({ type }: { type: string }) => {
  switch (type) {
    case 'throughput':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
        </svg>
      );
    case 'latency':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <circle cx="12" cy="12" r="10" />
          <polyline points="12 6 12 12 16 14" />
        </svg>
      );
    case 'memory':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <rect x="4" y="4" width="16" height="16" rx="2" />
          <rect x="8" y="8" width="8" height="8" rx="1" />
          <line x1="2" y1="12" x2="4" y2="12" />
          <line x1="20" y1="12" x2="22" y2="12" />
          <line x1="12" y1="2" x2="12" y2="4" />
          <line x1="12" y1="20" x2="12" y2="22" />
        </svg>
      );
    default:
      return null;
  }
};

export interface MetricCardProps {
  metric: PerformanceMetric;
}

export function MetricCard({ metric }: MetricCardProps) {
  const formatValue = (value: number, unit: string): string => {
    if (unit === 'ops/sec' && value >= 1000) {
      return `${(value / 1000).toFixed(0)}K`;
    }
    return value.toString();
  };

  const getImprovementPercentage = (): number | null => {
    if (!metric.comparison) return null;
    const improvement = ((metric.comparison.value - metric.value) / metric.comparison.value) * 100;
    return Math.round(improvement);
  };

  const improvement = getImprovementPercentage();
  const isLatency = metric.id === 'latency';
  const isBetter = improvement !== null && (isLatency ? improvement > 0 : improvement < 0);

  return (
    <div className="metric-card" data-testid={`metric-card-${metric.id}`}>
      <div className="metric-card__icon">
        <MetricIcon type={metric.id || ''} />
      </div>
      <div className="metric-card__content">
        <h3 className="metric-card__label" data-testid={`metric-label-${metric.id}`}>
          {metric.label}
        </h3>
        <div className="metric-card__value-container">
          <span className="metric-card__value" data-testid={`metric-value-${metric.id}`}>
            {formatValue(metric.value, metric.unit)}
          </span>
          <span className="metric-card__unit" data-testid={`metric-unit-${metric.id}`}>
            {metric.unit}
          </span>
        </div>
        {metric.comparison && (
          <div className="metric-card__comparison" data-testid={`metric-comparison-${metric.id}`}>
            <span className="metric-card__comparison-name">
              vs {metric.comparison.name}:
            </span>
            <span className="metric-card__comparison-value">
              {formatValue(metric.comparison.value, metric.unit)} {metric.unit}
            </span>
            {improvement !== null && (
              <span className={`metric-card__improvement ${isBetter ? 'metric-card__improvement--positive' : 'metric-card__improvement--negative'}`}>
                {isBetter ? '↓' : '↑'} {Math.abs(improvement)}% {isLatency ? 'faster' : (metric.id === 'memory' ? 'less' : 'more')}
              </span>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

export function Performance() {
  return (
    <section
      className="performance"
      id="performance"
      aria-labelledby="performance-heading"
      data-testid="performance-section"
    >
      <div className="container">
        <h2 id="performance-heading" className="performance__title">
          Performance Benchmarks
        </h2>
        <p className="performance__subtitle">
          Benchmarked against industry-standard key-value stores
        </p>

        <div className="performance__metrics" data-testid="performance-metrics">
          {performanceMetrics.map((metric) => (
            <MetricCard key={metric.id} metric={metric} />
          ))}
        </div>

        <div className="performance__comparison" data-testid="performance-comparison">
          <h3 className="performance__comparison-title">Database Comparison</h3>
          <div className="performance__table-container">
            <table className="performance__table" data-testid="comparison-table">
              <thead>
                <tr>
                  <th scope="col">Database</th>
                  <th scope="col">Throughput (ops/sec)</th>
                  <th scope="col">Latency (ms)</th>
                  <th scope="col">Memory (MB)</th>
                </tr>
              </thead>
              <tbody>
                {databaseComparisons.map((db) => (
                  <tr key={db.id} data-testid={`comparison-row-${db.id}`}>
                    <td className={db.id === 'mirdb' ? 'performance__highlight' : ''}>
                      {db.name}
                    </td>
                    <td data-testid={`${db.id}-throughput`}>
                      {db.metrics.throughput.toLocaleString()}
                    </td>
                    <td data-testid={`${db.id}-latency`}>
                      {db.metrics.latency}
                    </td>
                    <td data-testid={`${db.id}-memory`}>
                      {db.metrics.memory}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="performance__disclaimer">
            * Benchmarks performed on identical hardware with default configurations.
            Results may vary based on workload and system configuration.
          </p>
        </div>
      </div>
    </section>
  );
}
