/**
 * Resources Section component.
 * Owner: Scenario 6 - Resources and Links Section
 *
 * Displays grid of resource cards:
 * - Documentation
 * - API Reference
 * - Examples
 * - Contributing Guide
 * - Issue Tracker
 * - License
 *
 * Requirements: REQ-8
 */

'use client';

import React from 'react';
import { BookOpen, Code, Folder, Users, Bug, Scale } from 'lucide-react';
import { Card } from '@/components/ui/Card';
import { resources } from '@/data/resources';

const iconMap: Record<string, React.ComponentType<{ className?: string; 'aria-hidden'?: boolean }>> = {
  Documentation: BookOpen,
  'API Reference': Code,
  Examples: Folder,
  'Contributing Guide': Users,
  'Issue Tracker': Bug,
  License: Scale,
};

export function Resources() {
  return (
    <section
      id="resources"
      className="py-16 px-4 bg-gray-50 dark:bg-slate-900"
      aria-labelledby="resources-heading"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="resources-heading"
          className="text-3xl md:text-4xl font-bold text-center text-primary-900 dark:text-white mb-4"
        >
          Resources
        </h2>
        <p className="text-center text-gray-600 dark:text-gray-300 mb-12 max-w-2xl mx-auto">
          Everything you need to get started and contribute to MirDB
        </p>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {resources.map((resource) => {
            const IconComponent = iconMap[resource.title];
            const linkId = resource.title.toLowerCase().replace(/\s+/g, '-');

            return (
              <a
                key={resource.title}
                href={resource.href}
                target={resource.external ? '_blank' : undefined}
                rel={resource.external ? 'noopener noreferrer' : undefined}
                className="block group"
                data-testid={`resource-link-${linkId}`}
              >
                <Card
                  hoverable
                  className="h-full resource-card"
                  data-testid={`resource-card-${linkId}`}
                >
                  <div className="flex items-start gap-4">
                    {IconComponent && (
                      <div className="flex-shrink-0 w-12 h-12 rounded-lg bg-primary-100 dark:bg-primary-900 flex items-center justify-center">
                        <IconComponent
                          className="w-6 h-6 text-primary-600 dark:text-primary-400"
                          aria-hidden={true}
                        />
                      </div>
                    )}
                    <div className="flex-1 min-w-0">
                      <h3 className="text-lg font-semibold text-primary-900 dark:text-white mb-1 group-hover:text-accent-600 transition-colors">
                        {resource.title}
                      </h3>
                      <p className="text-sm text-gray-600 dark:text-gray-300">
                        {resource.description}
                      </p>
                    </div>
                  </div>
                  <span className="sr-only">{`${resource.title} - opens in new tab`}</span>
                </Card>
              </a>
            );
          })}
        </div>
      </div>
    </section>
  );
}
