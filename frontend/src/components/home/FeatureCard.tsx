/**
 * Feature Card Component.
 * Owner: Scenario 6 - Features Section with Analytics Showcase
 *
 * Individual feature card displaying:
 * - Title
 * - Description
 * - Visualization (SVG chart/map mockup)
 * - Subtle parallax scroll effect
 *
 * Uses: GlassMorphismCard styling pattern
 */
import { motion, useScroll, useTransform } from 'framer-motion';
import { useRef } from 'react';
import type { FeatureCardData } from '../../types/home';

interface FeatureCardProps extends FeatureCardData {
  className?: string;
  index?: number;
}

export function FeatureCard({
  title,
  description,
  visualization,
  className = '',
  index = 0,
}: FeatureCardProps) {
  const cardRef = useRef<HTMLDivElement>(null);
  const { scrollYProgress } = useScroll({
    target: cardRef,
    offset: ['start end', 'end start'],
  });

  // Subtle parallax effect - visualization moves slightly different from card
  const y = useTransform(scrollYProgress, [0, 1], [20, -20]);

  const cardVariants = {
    hidden: {
      opacity: 0,
      y: 30,
    },
    visible: {
      opacity: 1,
      y: 0,
      transition: {
        duration: 0.5,
        delay: index * 0.15,
        ease: 'easeOut',
      },
    },
  };

  return (
    <motion.div
      ref={cardRef}
      className={`card bg-base-200/50 backdrop-blur-sm border border-base-300 shadow-lg hover:shadow-xl transition-shadow duration-300 overflow-hidden ${className}`}
      variants={cardVariants}
      initial="hidden"
      whileInView="visible"
      viewport={{ once: true, margin: '-50px' }}
      data-testid="feature-card"
    >
      <div className="card-body p-6">
        {/* Visualization container with parallax */}
        <motion.div
          className="rounded-lg overflow-hidden mb-4 text-primary"
          style={{ y }}
          data-testid="feature-visualization"
        >
          {visualization}
        </motion.div>

        {/* Title */}
        <h3
          className="card-title text-lg font-semibold text-base-content"
          data-testid="feature-card-title"
        >
          {title}
        </h3>

        {/* Description - using /80 opacity for WCAG AA contrast compliance */}
        <p
          className="text-sm text-base-content/80 mt-2"
          data-testid="feature-card-description"
        >
          {description}
        </p>
      </div>
    </motion.div>
  );
}
