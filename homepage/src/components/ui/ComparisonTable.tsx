/**
 * Comparison table component.
 * Owner: Scenario 6 - Comparison Section
 *
 * Displays a table comparing MirDB vs alternatives with:
 * - Table header with product names
 * - Feature rows (Persistence, Protocol, etc.)
 * - Visual indicators (checkmarks, crosses)
 * - Responsive design
 */
import React from 'react';
import styles from './ComparisonTable.module.css';
import {
  ComparisonFeature,
  ComparisonProduct,
} from '@/data/comparison';

export interface ComparisonTableProps {
  features: ComparisonFeature[];
  products: ComparisonProduct[];
}

/**
 * Renders the feature value as a visual indicator
 */
function FeatureValue({ value }: { value: boolean | string }) {
  if (typeof value === 'boolean') {
    return (
      <span
        className={`${styles.featureIcon} ${value ? styles.check : styles.cross}`}
        role="img"
        aria-label={value ? 'Yes' : 'No'}
        data-testid={value ? 'check-icon' : 'cross-icon'}
      >
        {value ? '✓' : '✗'}
      </span>
    );
  }

  return (
    <span className={styles.text} data-testid="feature-text">
      {value}
    </span>
  );
}

export function ComparisonTable({ features, products }: ComparisonTableProps) {
  return (
    <div className={styles.tableWrapper} data-testid="comparison-table">
      <table className={styles.table} role="table">
        <thead>
          <tr className={styles.headerRow}>
            <th className={styles.headerCell} scope="col">
              Feature
            </th>
            {products.map((product) => (
              <th
                key={product.name}
                className={`${styles.headerCell} ${product.highlighted ? styles.highlightedHeader : ''}`}
                scope="col"
                data-testid={`product-header-${product.name.toLowerCase()}`}
              >
                {product.name}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {features.map((feature) => (
            <tr key={feature.name} className={styles.featureRow}>
              <td
                className={styles.featureCell}
                data-testid={`feature-row-${feature.name}`}
              >
                {feature.description}
              </td>
              {products.map((product) => (
                <td
                  key={`${product.name}-${feature.name}`}
                  className={`${styles.featureCell} ${product.highlighted ? styles.highlightedCell : ''}`}
                  data-testid={`cell-${product.name.toLowerCase()}-${feature.name}`}
                >
                  <FeatureValue value={product.features[feature.name]} />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default ComparisonTable;
