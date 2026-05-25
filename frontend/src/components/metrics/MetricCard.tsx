/**
 * MetricCard sub-component.
 *
 * Displays a single metric with a label and formatted value.
 * Supports smooth value transitions via CSS.
 */

import React from 'react';

export interface MetricCardProps {
  label: string;
  value: string;
  testId: string;
}

const MetricCard: React.FC<MetricCardProps> = ({ label, value, testId }) => {
  return (
    <div
      data-testid={testId}
      className="metric-card bg-gray-50 dark:bg-gray-800 rounded-lg p-6 shadow-sm border border-gray-200 dark:border-gray-700 transition-all duration-300 hover:shadow-md"
    >
      <p
        data-testid={`${testId}-label`}
        className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-1"
      >
        {label}
      </p>
      <p
        data-testid={`${testId}-value`}
        className="text-2xl font-bold text-gray-900 dark:text-white transition-opacity duration-500"
      >
        {value}
      </p>
    </div>
  );
};

export default MetricCard;
