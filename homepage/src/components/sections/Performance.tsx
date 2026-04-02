/**
 * Performance section with benchmarks.
 * Owner: Scenario 5 - Performance Section Display
 *
 * Displays MirDB performance benchmarks with hardware context
 * and test methodology information per REQ-6 and User Story 5.
 */
import React from 'react';
import { Container } from '@/components/common/Container';
import { PerformanceChart, PerformanceMetric } from '@/components/ui/PerformanceChart';
import { SECTION_IDS } from '@/utils/constants';
import styles from './Performance.module.css';

const benchmarkMetrics: PerformanceMetric[] = [
  {
    label: 'Read Operations',
    value: 150000,
    unit: 'ops/sec',
    maxValue: 200000,
  },
  {
    label: 'Write Operations',
    value: 120000,
    unit: 'ops/sec',
    maxValue: 200000,
  },
  {
    label: 'Average Latency',
    value: 0.5,
    unit: 'ms',
    maxValue: 2,
  },
  {
    label: 'P99 Latency',
    value: 1.2,
    unit: 'ms',
    maxValue: 5,
  },
];

const testContext = {
  hardware: 'Intel Core i7-12700K, 32GB DDR5, NVMe SSD',
  methodology: 'Benchmarked with 1M key-value pairs, 256-byte values, using memcached protocol',
  note: 'Results may vary based on hardware configuration and workload characteristics',
};

export function Performance() {
  return (
    <section
      id={SECTION_IDS.performance}
      className={styles.section}
      aria-labelledby="performance-heading"
      data-testid="performance-section"
    >
      <Container>
        <h2 id="performance-heading" className={styles.heading}>
          Performance
        </h2>
        <p className={styles.subtitle}>
          MirDB delivers high-throughput key-value operations with low latency
        </p>

        <div className={styles.content}>
          <div className={styles.chartWrapper}>
            <PerformanceChart metrics={benchmarkMetrics} title="Benchmark Results" />
          </div>

          <div className={styles.contextBox} data-testid="performance-context">
            <h3 className={styles.contextHeading}>Test Environment</h3>
            <dl className={styles.contextList}>
              <div className={styles.contextItem}>
                <dt className={styles.contextLabel}>Hardware</dt>
                <dd className={styles.contextValue} data-testid="hardware-specs">
                  {testContext.hardware}
                </dd>
              </div>
              <div className={styles.contextItem}>
                <dt className={styles.contextLabel}>Methodology</dt>
                <dd className={styles.contextValue} data-testid="test-methodology">
                  {testContext.methodology}
                </dd>
              </div>
            </dl>
            <p className={styles.note} data-testid="performance-note">
              {testContext.note}
            </p>
          </div>
        </div>
      </Container>
    </section>
  );
}
