/**
 * Features section component.
 * Owner: Scenario 4 - Features Section
 *
 * Displays a grid of 6 MirDB features, each with an icon, title, and description.
 * The grid is responsive:
 * - Desktop (lg+): 3 columns
 * - Tablet (md): 2 columns
 * - Mobile: 1 column
 */

import React from 'react';
import { Card } from '../common/Card';
import { features } from '../../data/features';
import type { Feature } from '../../types';

interface FeatureCardProps {
  feature: Feature;
}

const FeatureCard: React.FC<FeatureCardProps> = ({ feature }) => {
  return (
    <Card className="hover:shadow-lg hover:-translate-y-1 transition-all duration-200">
      <div className="flex flex-col items-center text-center" data-testid="feature-card">
        <span
          className="text-4xl mb-4"
          role="img"
          aria-label={`${feature.title} icon`}
        >
          {feature.icon}
        </span>
        <h3 className="text-xl font-semibold mb-2 text-gray-900 dark:text-white">
          {feature.title}
        </h3>
        <p className="text-gray-600 dark:text-gray-400">{feature.description}</p>
      </div>
    </Card>
  );
};

export const Features: React.FC = () => {
  return (
    <section
      id="features"
      className="py-20 px-4 bg-gray-50 dark:bg-gray-900"
      aria-labelledby="features-heading"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="features-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-4 text-gray-900 dark:text-white"
        >
          Features
        </h2>
        <p className="text-center text-gray-600 dark:text-gray-400 mb-12 max-w-2xl mx-auto">
          MirDB combines the simplicity of Memcached with the durability of
          persistent storage, powered by modern Rust architecture.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {features.map((feature) => (
            <FeatureCard key={feature.title} feature={feature} />
          ))}
        </div>
      </div>
    </section>
  );
};
