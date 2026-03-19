/**
 * Features Section component.
 * Owner: Scenario 2 - Value Proposition Features Section
 *
 * Displays three feature cards in grid layout:
 * - Memcached Compatible
 * - Persistent Storage (LSM tree)
 * - Rust Performance
 *
 * Requirements: REQ-2, REQ-3, REQ-4
 */

'use client';

import React from 'react';
import { Plug, Database, Zap } from 'lucide-react';
import { Card } from '@/components/ui/Card';
import { features } from '@/data/features';

const iconMap: Record<string, React.ComponentType<{ className?: string; 'aria-hidden'?: boolean }>> = {
  Plug,
  Database,
  Zap,
};

export function Features() {
  return (
    <section
      id="features"
      className="py-16 px-4 bg-gray-50 dark:bg-slate-900"
      aria-labelledby="features-heading"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="features-heading"
          className="text-3xl md:text-4xl font-bold text-center text-primary-900 dark:text-white mb-4"
        >
          Why Choose MirDB?
        </h2>
        <p className="text-center text-gray-600 dark:text-gray-300 mb-12 max-w-2xl mx-auto">
          The best of both worlds: memcached compatibility with persistent storage
        </p>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
          {features.map((feature) => {
            const IconComponent = iconMap[feature.icon];

            return (
              <Card
                key={feature.id}
                hoverable
                className="text-center feature-card"
                data-testid={`feature-card-${feature.id}`}
              >
                <div className="flex justify-center mb-4">
                  {IconComponent && (
                    <div className="w-16 h-16 rounded-full bg-primary-100 dark:bg-primary-900 flex items-center justify-center">
                      <IconComponent
                        className="w-8 h-8 text-primary-600 dark:text-primary-400"
                        aria-hidden={true}
                      />
                    </div>
                  )}
                </div>
                <span className="sr-only">{`${feature.title} icon`}</span>
                <h3 className="text-xl font-semibold text-primary-900 dark:text-white mb-3">
                  {feature.title}
                </h3>
                <p className="text-gray-600 dark:text-gray-300">
                  {feature.description}
                </p>
              </Card>
            );
          })}
        </div>
      </div>
    </section>
  );
}
