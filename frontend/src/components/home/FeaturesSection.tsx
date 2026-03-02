/**
 * Features Section Component.
 * Owner: Scenario 6 - Features Section with Analytics Showcase
 *
 * Displays analytics features with mock visualizations:
 * - Section heading: "Powerful Analytics at Your Fingertips"
 * - Three feature cards in grid layout:
 *   1. "Real-time Click Tracking" with line chart mockup
 *   2. "Geographic Insights" with map visualization
 *   3. "Device & Browser Breakdown" with pie chart mockup
 *
 * Requirements: REQ-2, REQ-7
 * Min height: 500px
 * Charts: Static SVG mockups (NOT Recharts) for performance
 */
import { motion } from 'framer-motion';
import { FeatureCard } from './FeatureCard';
import { LineChartMockup, MapMockup, PieChartMockup } from '../charts';
import type { FeatureCardData } from '../../types/home';

interface FeaturesSectionProps {
  className?: string;
}

const features: FeatureCardData[] = [
  {
    title: 'Real-time Click Tracking',
    description:
      'Monitor your link performance in real-time. See exactly when and how often your links are clicked with live updating dashboards.',
    visualization: <LineChartMockup animate />,
  },
  {
    title: 'Geographic Insights',
    description:
      'Understand where your audience comes from. Visualize click data on an interactive world map with detailed location breakdowns.',
    visualization: <MapMockup animate />,
  },
  {
    title: 'Device & Browser Breakdown',
    description:
      'Know your audience better. See which devices and browsers your visitors use to optimize your content delivery.',
    visualization: <PieChartMockup animate />,
  },
];

const headingVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: { duration: 0.5, ease: 'easeOut' },
  },
};

export function FeaturesSection({ className = '' }: FeaturesSectionProps) {
  return (
    <section
      className={`py-16 px-4 min-h-[500px] ${className}`}
      aria-labelledby="features-heading"
      data-testid="features-section"
    >
      <div className="max-w-6xl mx-auto">
        {/* Section heading */}
        <motion.div
          className="text-center mb-12"
          variants={headingVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-50px' }}
        >
          <h2
            id="features-heading"
            className="text-3xl md:text-4xl font-bold text-base-content"
            data-testid="features-heading"
          >
            Powerful Analytics at Your Fingertips
          </h2>
          <p className="mt-4 text-lg text-base-content/70 max-w-2xl mx-auto">
            Get detailed insights into your link performance with our comprehensive analytics suite.
          </p>
        </motion.div>

        {/* Feature cards grid */}
        <div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
          data-testid="features-grid"
        >
          {features.map((feature, index) => (
            <FeatureCard
              key={feature.title}
              {...feature}
              index={index}
            />
          ))}
        </div>
      </div>
    </section>
  );
}
