/**
 * Features Section Component.
 * Owner: Scenario 3 - Features Section Display
 *
 * Displays a grid of 4+ feature cards highlighting the product's key features:
 * URL Shortening, Click Analytics, User Dashboard, Secure Authentication
 *
 * Uses GlassMorphismCard component for styling and Framer Motion for animations.
 * Responsive: 4 columns desktop, 2 columns mobile
 */

import React from 'react';
import { motion } from 'framer-motion';
import { Link2, BarChart3, LayoutDashboard, Shield } from 'lucide-react';
import { GlassMorphismCard } from '../GlassMorphismCard';

interface Feature {
  id: string;
  icon: React.ReactNode;
  title: string;
  description: string;
}

const features: Feature[] = [
  {
    id: 'url-shortening',
    icon: <Link2 className="w-8 h-8 text-primary" aria-hidden="true" />,
    title: 'URL Shortening',
    description: 'Create short, memorable links from long URLs in seconds. Share them anywhere with ease.',
  },
  {
    id: 'click-analytics',
    icon: <BarChart3 className="w-8 h-8 text-primary" aria-hidden="true" />,
    title: 'Click Analytics',
    description: 'Track every click with detailed analytics. See where your audience comes from and when they engage.',
  },
  {
    id: 'user-dashboard',
    icon: <LayoutDashboard className="w-8 h-8 text-primary" aria-hidden="true" />,
    title: 'User Dashboard',
    description: 'Manage all your links in one place. View stats, edit destinations, and organize with ease.',
  },
  {
    id: 'secure-authentication',
    icon: <Shield className="w-8 h-8 text-primary" aria-hidden="true" />,
    title: 'Secure Authentication',
    description: 'Your data is protected with industry-standard security. Rest easy knowing your links are safe.',
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

const cardVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.5,
      ease: 'easeOut',
    },
  },
};

export const FeaturesSection: React.FC = () => {
  return (
    <section
      data-testid="features-section"
      className="py-16 px-4 md:px-8"
      aria-labelledby="features-heading"
    >
      <div className="max-w-7xl mx-auto">
        <h2
          id="features-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12"
        >
          Powerful Features
        </h2>
        <motion.div
          className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-100px' }}
        >
          {features.map((feature) => (
            <motion.div key={feature.id} variants={cardVariants}>
              <GlassMorphismCard
                data-testid={`feature-card-${feature.id}`}
                className="p-6 h-full flex flex-col items-center text-center hover:scale-105 transition-transform duration-300"
              >
                <div
                  data-testid={`feature-icon-${feature.id}`}
                  className="mb-4 p-3 rounded-full bg-primary/10"
                >
                  {feature.icon}
                </div>
                <h3
                  data-testid={`feature-title-${feature.id}`}
                  className="text-xl font-semibold mb-2"
                >
                  {feature.title}
                </h3>
                <p
                  data-testid={`feature-description-${feature.id}`}
                  className="text-base-content/70"
                >
                  {feature.description}
                </p>
              </GlassMorphismCard>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  );
};

export default FeaturesSection;
