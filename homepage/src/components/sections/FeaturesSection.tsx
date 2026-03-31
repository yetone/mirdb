/**
 * Features Section Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Displays feature cards in a responsive grid:
 * - 3 columns on desktop, 1 column on mobile
 * - Status badges for each feature
 * - Feature icons and descriptions
 *
 * Requirements: REQ-2, Story 2 (Feature Understanding)
 */

import { Check, Clock, Network, HardDrive, GitBranch, Zap, Layers, Users, LucideIcon } from 'lucide-react';
import type { Feature } from '../../types';
import { Card } from '../ui/Card';
import { Badge } from '../ui/Badge';

interface FeaturesSectionProps {
  features: Feature[];
}

const iconMap: Record<string, LucideIcon> = {
  Network,
  HardDrive,
  GitBranch,
  Zap,
  Layers,
  Users,
};

function FeatureCard({ feature }: { feature: Feature }) {
  const IconComponent = feature.icon ? iconMap[feature.icon] : null;
  const isImplemented = feature.status === 'implemented';

  return (
    <Card hover data-testid={`feature-card-${feature.id}`} className="flex flex-col h-full">
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center gap-3">
          {IconComponent && (
            <div className="p-2 bg-primary-100 dark:bg-primary-900/30 rounded-lg">
              <IconComponent className="w-5 h-5 text-primary-600 dark:text-primary-400" />
            </div>
          )}
          <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
            {feature.name}
          </h3>
        </div>
        {isImplemented ? (
          <Badge variant="success" data-testid={`status-badge-${feature.id}`}>
            <Check className="w-3 h-3 mr-1" aria-hidden="true" />
            <span>Implemented</span>
          </Badge>
        ) : (
          <Badge variant="warning" data-testid={`status-badge-${feature.id}`}>
            <Clock className="w-3 h-3 mr-1" aria-hidden="true" />
            <span>Upcoming</span>
          </Badge>
        )}
      </div>
      <p className="text-gray-600 dark:text-gray-400 text-sm leading-relaxed flex-grow">
        {feature.description}
      </p>
    </Card>
  );
}

export function FeaturesSection({ features }: FeaturesSectionProps) {
  return (
    <section
      id="features"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-gray-50 dark:bg-gray-900"
      aria-labelledby="features-heading"
    >
      <div className="max-w-7xl mx-auto">
        <div className="text-center mb-12">
          <h2
            id="features-heading"
            className="text-3xl font-bold text-gray-900 dark:text-gray-100 sm:text-4xl"
          >
            Features
          </h2>
          <p className="mt-4 text-lg text-gray-600 dark:text-gray-400 max-w-2xl mx-auto">
            MirDB combines the simplicity of Memcached with the durability of persistent storage.
          </p>
        </div>
        <div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
          data-testid="features-grid"
        >
          {features.map((feature) => (
            <FeatureCard key={feature.id} feature={feature} />
          ))}
        </div>
      </div>
    </section>
  );
}
