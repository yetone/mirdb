/**
 * Feature Card Component
 * Owner: Scenario 3 - Features Section Display
 *
 * Individual feature card with:
 * - Icon or visual indicator
 * - Title
 * - Description
 *
 * Uses GlassMorphismCard styling pattern.
 *
 * Expected props:
 * - title: string
 * - description: string
 * - icon: ReactNode
 *
 * Requirements: REQ-2, REQ-4
 */

import type { ReactNode } from 'react';
import { GlassMorphismCard } from '../GlassMorphismCard';

export interface FeatureCardProps {
  title: string;
  description: string;
  icon: ReactNode;
}

/**
 * FeatureCard - Displays a single feature with icon, title, and description
 * Uses glass morphism styling for a modern, futuristic appearance.
 */
export function FeatureCard({ title, description, icon }: FeatureCardProps) {
  return (
    <GlassMorphismCard className="p-6 h-full">
      <div data-testid="feature-card" className="flex flex-col items-center text-center space-y-4">
        {/* Icon container with gradient background */}
        <div
          data-testid="feature-icon"
          className="w-16 h-16 rounded-full bg-gradient-to-br from-purple-500 to-pink-500 flex items-center justify-center text-white shadow-lg"
        >
          {icon}
        </div>

        {/* Feature title */}
        <h3
          data-testid="feature-title"
          className="text-xl font-bold text-white"
        >
          {title}
        </h3>

        {/* Feature description */}
        <p
          data-testid="feature-description"
          className="text-gray-300 leading-relaxed"
        >
          {description}
        </p>
      </div>
    </GlassMorphismCard>
  );
}
