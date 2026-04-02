/**
 * PerformanceChart component for displaying performance benchmarks.
 * Owner: Scenario 5 - Performance Section Display
 *
 * Displays a visual bar chart of benchmark metrics with labels and values.
 */
import React from 'react';
import styles from './PerformanceChart.module.css';

export interface PerformanceMetric {
  label: string;
  value: number;
  unit: string;
  maxValue: number;
}

interface PerformanceChartProps {
  metrics: PerformanceMetric[];
  title?: string;
}

export function PerformanceChart({ metrics, title }: PerformanceChartProps) {
  return (
    <div className={styles.chart} data-testid="performance-chart" role="figure" aria-label={title || 'Performance metrics'}>
      {title && <h3 className={styles.chartTitle}>{title}</h3>}
      <div className={styles.metricsContainer}>
        {metrics.map((metric) => {
          const percentage = Math.min((metric.value / metric.maxValue) * 100, 100);
          return (
            <div key={metric.label} className={styles.metric} data-testid="performance-metric">
              <div className={styles.metricHeader}>
                <span className={styles.metricLabel}>{metric.label}</span>
                <span className={styles.metricValue} data-testid="metric-value">
                  {metric.value.toLocaleString()} {metric.unit}
                </span>
              </div>
              <div className={styles.barContainer} role="progressbar" aria-valuenow={metric.value} aria-valuemin={0} aria-valuemax={metric.maxValue} aria-label={`${metric.label}: ${metric.value.toLocaleString()} ${metric.unit}`}>
                <div
                  className={styles.bar}
                  style={{ width: `${percentage}%` }}
                  data-testid="metric-bar"
                />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
