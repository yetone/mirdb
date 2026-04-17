/**
 * Feature Card Component
 * Owner: Scenario 3 - Features Section Display
 *
 * Individual feature card with:
 * - Icon (from lucide-react or @heroicons/react)
 * - Title (heading element)
 * - Description text
 * - Hover effects
 *
 * Related requirements: REQ-4, REQ-10, NFR-3
 */

import { motion } from 'framer-motion';
import type { FeatureCardProps } from '@/types/homepage';

export function FeatureCard({ icon, title, description }: FeatureCardProps) {
  return (
    <motion.div
      className="card bg-base-100 shadow-xl hover:shadow-2xl transition-shadow duration-300"
      data-testid="feature-card"
      whileHover={{ y: -5 }}
      transition={{ duration: 0.2 }}
    >
      <div className="card-body items-center text-center">
        <div
          className="w-16 h-16 flex items-center justify-center rounded-full bg-primary/10 text-primary mb-4"
          data-testid="feature-icon"
          aria-hidden="true"
        >
          {icon}
        </div>
        <h3
          className="card-title text-xl font-semibold"
          data-testid="feature-title"
        >
          {title}
        </h3>
        <p
          className="text-base-content/70"
          data-testid="feature-description"
        >
          {description}
        </p>
      </div>
    </motion.div>
  );
}
