/**
 * Social Proof Section Component
 * Owner: Scenario 5 - Social Proof Section
 *
 * Trust-building section with:
 * - Statistics display (users count, links shortened)
 * - Optional testimonial carousel
 * - "Trusted by" section if applicable
 *
 * Number formatting for large values (e.g., "10K+")
 *
 * Related requirements: REQ-6
 */

import { motion } from 'framer-motion';
import { Users, Link2, TrendingUp, Quote } from 'lucide-react';
import type { Statistic, Testimonial, SocialProofSectionProps } from '@/types/homepage';

/**
 * Format large numbers for display
 * Examples: 1000 -> "1K+", 1500000 -> "1.5M+", 1234567 -> "1,234,567"
 */
export function formatStatNumber(value: number, useCompact: boolean = true): string {
  if (!useCompact) {
    return value.toLocaleString();
  }

  if (value >= 1_000_000) {
    const millions = value / 1_000_000;
    return millions % 1 === 0 ? `${millions}M+` : `${millions.toFixed(1)}M+`;
  }

  if (value >= 1_000) {
    const thousands = value / 1_000;
    return thousands % 1 === 0 ? `${thousands}K+` : `${thousands.toFixed(1)}K+`;
  }

  return value.toLocaleString();
}

const defaultStatistics: Statistic[] = [
  {
    id: 'users',
    value: 50000,
    label: 'Active Users',
  },
  {
    id: 'links',
    value: 2500000,
    label: 'Links Shortened',
  },
  {
    id: 'clicks',
    value: 150000000,
    label: 'Total Clicks',
  },
];

const defaultTestimonials: Testimonial[] = [
  {
    id: 'testimonial-1',
    author: 'Sarah Johnson',
    role: 'Marketing Director',
    content:
      'This URL shortener has transformed how we track our marketing campaigns. The analytics are incredibly detailed.',
  },
  {
    id: 'testimonial-2',
    author: 'Michael Chen',
    role: 'Software Engineer',
    content:
      'Clean API, fast redirects, and the custom short codes feature is exactly what we needed for our product.',
  },
  {
    id: 'testimonial-3',
    author: 'Emily Rodriguez',
    role: 'Content Creator',
    content:
      'I use this for all my social media links. Being able to see where my audience comes from is invaluable.',
  },
];

const statIcons: Record<string, React.ReactNode> = {
  users: <Users className="w-6 h-6" />,
  links: <Link2 className="w-6 h-6" />,
  clicks: <TrendingUp className="w-6 h-6" />,
};

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.15,
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

interface StatisticCardProps {
  statistic: Statistic;
}

function StatisticCard({ statistic }: StatisticCardProps) {
  const icon = statIcons[statistic.id] || <TrendingUp className="w-6 h-6" />;
  const formattedValue = formatStatNumber(statistic.value);

  return (
    <div
      className="flex flex-col items-center p-6 bg-base-100 rounded-xl shadow-lg"
      data-testid="statistic-counter"
    >
      <div
        className="text-primary mb-3"
        data-testid="statistic-icon"
        aria-hidden="true"
      >
        {icon}
      </div>
      <div
        className="text-3xl md:text-4xl font-bold mb-2"
        data-testid="statistic-value"
      >
        {statistic.prefix}
        {formattedValue}
        {statistic.suffix}
      </div>
      <div
        className="text-base-content/70 text-sm"
        data-testid="statistic-label"
      >
        {statistic.label}
      </div>
    </div>
  );
}

interface TestimonialCardProps {
  testimonial: Testimonial;
}

function TestimonialCard({ testimonial }: TestimonialCardProps) {
  return (
    <div
      className="card bg-base-100 shadow-lg p-6"
      data-testid="testimonial-card"
    >
      <div className="flex items-start gap-4">
        <div
          className="flex-shrink-0 text-primary/50"
          aria-hidden="true"
        >
          <Quote className="w-8 h-8" />
        </div>
        <div className="flex-1">
          <blockquote
            className="text-base-content/80 mb-4"
            data-testid="testimonial-quote"
          >
            "{testimonial.content}"
          </blockquote>
          <div data-testid="testimonial-attribution">
            <div
              className="font-semibold"
              data-testid="testimonial-author"
            >
              {testimonial.author}
            </div>
            <div
              className="text-sm text-base-content/60"
              data-testid="testimonial-role"
            >
              {testimonial.role}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export function SocialProofSection({
  className = '',
  statistics = defaultStatistics,
  testimonials = defaultTestimonials,
}: SocialProofSectionProps) {
  return (
    <section
      className={`py-16 px-4 bg-base-200 ${className}`}
      data-testid="social-proof-section"
      aria-labelledby="social-proof-heading"
    >
      <div className="container mx-auto max-w-6xl">
        <div className="text-center mb-12">
          <h2
            id="social-proof-heading"
            className="text-3xl md:text-4xl font-bold mb-4"
          >
            Trusted by Thousands
          </h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Join the growing community of users who trust us with their links.
          </p>
        </div>

        {/* Statistics Grid */}
        <motion.div
          className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-16"
          data-testid="statistics-container"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-100px' }}
        >
          {statistics.map((stat) => (
            <motion.div key={stat.id} variants={itemVariants}>
              <StatisticCard statistic={stat} />
            </motion.div>
          ))}
        </motion.div>

        {/* Testimonials Grid */}
        {testimonials.length > 0 && (
          <div data-testid="testimonials-container">
            <h3 className="text-2xl font-bold text-center mb-8">
              What Our Users Say
            </h3>
            <motion.div
              className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
              variants={containerVariants}
              initial="hidden"
              whileInView="visible"
              viewport={{ once: true, margin: '-100px' }}
            >
              {testimonials.map((testimonial) => (
                <motion.div key={testimonial.id} variants={itemVariants}>
                  <TestimonialCard testimonial={testimonial} />
                </motion.div>
              ))}
            </motion.div>
          </div>
        )}
      </div>
    </section>
  );
}
