/**
 * Project Status Section component.
 * Owner: Scenario 4 - Project Status Section
 *
 * Features:
 * - Implemented features checklist with checkmarks
 * - Planned features (Raft) with "Coming Soon" badge
 * - CircleCI build status badge
 *
 * Requirements: REQ-5, REQ-15
 */

'use client';

import React from 'react';
import { Check, Clock } from 'lucide-react';
import { Card } from '@/components/ui/Card';
import { CIRCLECI_BADGE_URL } from '@/lib/constants';
import { implementedFeatures, plannedFeatures, CI_PIPELINE_URL } from '@/data/status';

export function ProjectStatus() {
  return (
    <section
      id="status"
      className="py-16 px-4 bg-white dark:bg-slate-800"
      aria-labelledby="status-heading"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="status-heading"
          className="text-3xl md:text-4xl font-bold text-center text-primary-900 dark:text-white mb-4"
        >
          Project Status
        </h2>
        <p className="text-center text-gray-600 dark:text-gray-300 mb-12 max-w-2xl mx-auto">
          Track the progress of MirDB features and development
        </p>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-8 mb-12">
          {/* Implemented Features */}
          <Card className="status-card" data-testid="implemented-features-card">
            <h3 className="text-xl font-semibold text-primary-900 dark:text-white mb-6 flex items-center">
              <Check className="w-6 h-6 text-green-500 mr-2" aria-hidden="true" />
              Implemented Features
            </h3>
            <ul className="space-y-3" role="list" aria-label="List of implemented features">
              {implementedFeatures.map((feature, index) => (
                <li
                  key={index}
                  className="flex items-center text-gray-700 dark:text-gray-300"
                  data-testid={`implemented-feature-${index}`}
                >
                  <Check
                    className="w-5 h-5 text-green-500 mr-3 flex-shrink-0"
                    aria-hidden="true"
                  />
                  <span>{feature.name}</span>
                  <span className="sr-only">(implemented)</span>
                </li>
              ))}
            </ul>
          </Card>

          {/* Planned Features */}
          <Card className="status-card" data-testid="planned-features-card">
            <h3 className="text-xl font-semibold text-primary-900 dark:text-white mb-6 flex items-center">
              <Clock className="w-6 h-6 text-amber-500 mr-2" aria-hidden="true" />
              Coming Soon
            </h3>
            <ul className="space-y-3" role="list" aria-label="List of planned features">
              {plannedFeatures.map((feature, index) => (
                <li
                  key={index}
                  className="flex items-center text-gray-700 dark:text-gray-300"
                  data-testid={`planned-feature-${index}`}
                >
                  <Clock
                    className="w-5 h-5 text-amber-500 mr-3 flex-shrink-0"
                    aria-hidden="true"
                  />
                  <span>{feature.name}</span>
                  <span
                    className="ml-2 px-2 py-0.5 text-xs font-medium bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200 rounded-full"
                    data-testid="coming-soon-badge"
                  >
                    Coming Soon
                  </span>
                </li>
              ))}
            </ul>
          </Card>
        </div>

        {/* CI Badge */}
        <div className="text-center" data-testid="ci-badge-container">
          <h3 className="text-lg font-semibold text-primary-900 dark:text-white mb-4">
            Build Status
          </h3>
          <a
            href={CI_PIPELINE_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-block hover:opacity-80 transition-opacity"
            aria-label="View CircleCI build status (opens in new tab)"
            data-testid="ci-badge-link"
          >
            <img
              src={CIRCLECI_BADGE_URL}
              alt="CircleCI Build Status"
              className="h-6"
              data-testid="ci-badge-image"
            />
          </a>
        </div>
      </div>
    </section>
  );
}
