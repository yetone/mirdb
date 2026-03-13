/**
 * Feature Card Component
 * Owner: Scenario 4 - Features Section Display
 *
 * Reusable card component for displaying feature information.
 */

import type { ReactNode } from 'react';

export interface CardProps {
  title: string;
  description: string;
  icon?: ReactNode;
  className?: string;
}

export function Card({ title, description, icon, className = '' }: CardProps) {
  return (
    <article
      data-testid="feature-card"
      className={`bg-white dark:bg-gray-700 p-6 rounded-lg shadow hover:shadow-lg transition-shadow ${className}`}
    >
      {icon && (
        <div className="text-purple-600 dark:text-purple-400 mb-4" aria-hidden="true">
          {icon}
        </div>
      )}
      <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
        {title}
      </h3>
      <p className="text-gray-600 dark:text-gray-300">
        {description}
      </p>
    </article>
  );
}
