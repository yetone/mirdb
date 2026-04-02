/**
 * Comparison section with alternatives table.
 * Owner: Scenario 6 - Comparison Section
 *
 * Displays:
 * - Section heading "Why MirDB?" or "Comparison"
 * - ComparisonTable component
 * - MirDB vs Memcached vs Redis
 */
import React from 'react';
import styles from './Comparison.module.css';
import { ComparisonTable } from '@/components/ui/ComparisonTable';
import {
  COMPARISON_FEATURES,
  COMPARISON_PRODUCTS,
  COMPARISON_SECTION_CONTENT,
} from '@/data/comparison';
import { SECTION_IDS } from '@/utils/constants';

export function Comparison() {
  return (
    <section
      id={SECTION_IDS.comparison}
      className={styles.section}
      data-testid="comparison-section"
      aria-labelledby="comparison-heading"
    >
      <div className={styles.container}>
        <header className={styles.header}>
          <h2 id="comparison-heading" className={styles.title}>
            {COMPARISON_SECTION_CONTENT.title}
          </h2>
          <p className={styles.subtitle}>
            {COMPARISON_SECTION_CONTENT.subtitle}
          </p>
          <p className={styles.description}>
            {COMPARISON_SECTION_CONTENT.description}
          </p>
        </header>

        <div className={styles.tableContainer}>
          <ComparisonTable
            features={COMPARISON_FEATURES}
            products={COMPARISON_PRODUCTS}
          />
        </div>
      </div>
    </section>
  );
}

export default Comparison;
