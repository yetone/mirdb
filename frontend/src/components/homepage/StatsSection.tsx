/**
 * Stats/Trust Indicators Section Component
 * Owner: Scenario 4 - Trust Indicators and Statistics
 *
 * Requirements: REQ-9
 *
 * Expected functionality:
 * - Display trust indicators/statistics
 * - Example metrics:
 *   - Total URLs shortened
 *   - Total clicks tracked
 *   - Active users (optional)
 * - Animated number counters (optional)
 * - Can use placeholder/mock data initially
 * - Smooth scroll anchor: #stats
 *
 * Note: This is a "Could" priority feature
 */

import { motion, useInView } from 'framer-motion';
import { useRef, useState, useEffect } from 'react';
import { Link2, MousePointer2, Users } from 'lucide-react';
import { GlassMorphismCard } from '../GlassMorphismCard';

interface Stat {
  id: number;
  icon: React.ReactNode;
  value: number;
  label: string;
  suffix?: string;
}

// Default statistics (can be replaced with real API data in the future)
const defaultStats: Stat[] = [
  {
    id: 1,
    icon: <Link2 className="w-8 h-8" aria-hidden="true" />,
    value: 1500000,
    label: 'URLs Created',
  },
  {
    id: 2,
    icon: <MousePointer2 className="w-8 h-8" aria-hidden="true" />,
    value: 25000000,
    label: 'Clicks Tracked',
  },
  {
    id: 3,
    icon: <Users className="w-8 h-8" aria-hidden="true" />,
    value: 50000,
    label: 'Active Users',
    suffix: '+',
  },
];

/**
 * Formats a number with appropriate formatting:
 * - Numbers >= 1,000,000 are abbreviated (e.g., 1.5M)
 * - Numbers >= 1,000 use comma separators (e.g., 50,000)
 */
export function formatStatNumber(value: number): string {
  if (value >= 1000000) {
    const millions = value / 1000000;
    // If it's a clean million (e.g., 1000000 -> 1M)
    if (millions === Math.floor(millions)) {
      return `${Math.floor(millions)}M`;
    }
    // Otherwise show one decimal (e.g., 1500000 -> 1.5M)
    return `${millions.toFixed(1).replace(/\.0$/, '')}M`;
  }
  // Use comma separator for thousands
  return value.toLocaleString('en-US');
}

interface AnimatedCounterProps {
  value: number;
  duration?: number;
  suffix?: string;
}

/**
 * Animated counter that counts up to the target value
 * Respects reduced motion preferences
 */
function AnimatedCounter({ value, duration = 2000, suffix = '' }: AnimatedCounterProps) {
  const [displayValue, setDisplayValue] = useState(0);
  const ref = useRef<HTMLSpanElement>(null);
  const isInView = useInView(ref, { once: true, margin: '-50px' });

  // Check for reduced motion preference
  const prefersReducedMotion =
    typeof window !== 'undefined' &&
    window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;

  useEffect(() => {
    if (!isInView) return;

    // If user prefers reduced motion, show final value immediately
    if (prefersReducedMotion) {
      setDisplayValue(value);
      return;
    }

    // Animate the counter
    const startTime = performance.now();
    const startValue = 0;

    function animate(currentTime: number) {
      const elapsed = currentTime - startTime;
      const progress = Math.min(elapsed / duration, 1);

      // Ease out cubic for smoother animation
      const easeOut = 1 - Math.pow(1 - progress, 3);
      const currentValue = Math.floor(startValue + (value - startValue) * easeOut);

      setDisplayValue(currentValue);

      if (progress < 1) {
        requestAnimationFrame(animate);
      } else {
        setDisplayValue(value);
      }
    }

    requestAnimationFrame(animate);
  }, [isInView, value, duration, prefersReducedMotion]);

  return (
    <span ref={ref} data-testid="stat-value">
      {formatStatNumber(displayValue)}
      {suffix}
    </span>
  );
}

interface StatsSectionProps {
  stats?: Stat[];
}

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
  hidden: { opacity: 0, y: 30 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.5,
    },
  },
};

export function StatsSection({ stats = defaultStats }: StatsSectionProps) {
  return (
    <section
      id="stats"
      data-testid="stats-section"
      aria-labelledby="stats-heading"
      className="py-16 md:py-24 px-4 md:px-8 bg-base-200/50"
    >
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-12">
          <h2
            id="stats-heading"
            className="text-3xl md:text-4xl font-bold mb-4"
          >
            Trusted by Thousands
          </h2>
          <p className="text-base-content/70 text-lg max-w-2xl mx-auto">
            Join our growing community of link creators and marketers
          </p>
        </div>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-3 gap-6 md:gap-8"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-100px' }}
        >
          {stats.map((stat) => (
            <motion.div key={stat.id} variants={itemVariants}>
              <GlassMorphismCard
                className="p-8 text-center"
                data-testid="stat-card"
              >
                <div
                  className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center mx-auto mb-4 text-primary"
                  data-testid="stat-icon"
                >
                  {stat.icon}
                </div>
                <div
                  className="text-4xl md:text-5xl font-bold text-primary mb-2"
                  data-testid="stat-number"
                >
                  <AnimatedCounter value={stat.value} suffix={stat.suffix} />
                </div>
                <p
                  className="text-base-content/70 text-lg font-medium"
                  data-testid="stat-label"
                >
                  {stat.label}
                </p>
              </GlassMorphismCard>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  );
}

export default StatsSection;
