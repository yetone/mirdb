/**
 * Features Section Component
 * Owner: Scenario 3 - Features Section Display
 *
 * Grid display of 3-5 key product features:
 * - Instant URL shortening
 * - Click analytics and insights
 * - QR code generation
 * - Custom short codes
 * - Geographic tracking
 *
 * Uses FeatureCard component for each feature.
 *
 * Related requirements: REQ-4, REQ-10, NFR-3
 */

import { motion } from 'framer-motion';
import { Link, BarChart3, QrCode, PenLine, Globe } from 'lucide-react';
import { FeatureCard } from './FeatureCard';
import type { Feature } from '@/types/homepage';

const features: Feature[] = [
  {
    id: 'instant-shortening',
    icon: <Link className="w-8 h-8" />,
    title: 'Instant URL Shortening',
    description:
      'Transform long URLs into short, memorable links in seconds. No signup required to get started.',
  },
  {
    id: 'analytics',
    icon: <BarChart3 className="w-8 h-8" />,
    title: 'Click Analytics',
    description:
      'Track every click with detailed analytics. See when, where, and how your links are performing.',
  },
  {
    id: 'qr-codes',
    icon: <QrCode className="w-8 h-8" />,
    title: 'QR Code Generation',
    description:
      'Generate QR codes for your short links instantly. Perfect for print materials and offline marketing.',
  },
  {
    id: 'custom-codes',
    icon: <PenLine className="w-8 h-8" />,
    title: 'Custom Short Codes',
    description:
      'Create branded, memorable short links with custom aliases that reflect your brand identity.',
  },
  {
    id: 'geo-tracking',
    icon: <Globe className="w-8 h-8" />,
    title: 'Geographic Tracking',
    description:
      'See where your audience is located with geographic click tracking and insights by region.',
  },
];

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.1,
    },
  },
};

const itemVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.5,
    },
  },
};

export function FeaturesSection() {
  return (
    <section
      className="py-16 px-4 bg-base-200"
      data-testid="features-section"
      aria-labelledby="features-heading"
    >
      <div className="container mx-auto max-w-6xl">
        <div className="text-center mb-12">
          <h2
            id="features-heading"
            className="text-3xl md:text-4xl font-bold mb-4"
          >
            Powerful Features
          </h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Everything you need to shorten, share, and track your links effectively.
          </p>
        </div>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-100px' }}
        >
          {features.map((feature) => (
            <motion.div key={feature.id} variants={itemVariants}>
              <FeatureCard
                icon={feature.icon}
                title={feature.title}
                description={feature.description}
              />
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  );
}
