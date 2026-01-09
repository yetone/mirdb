import { motion, useReducedMotion } from 'framer-motion'
import { HiLink, HiCursorClick, HiUsers } from 'react-icons/hi'
import { formatNumber } from '../utils/formatNumber'

/**
 * Interface for a single statistic item
 */
export interface Statistic {
  id: string
  label: string
  value: number
  icon: React.ReactNode
}

/**
 * Props for the StatisticsSection component
 */
export interface StatisticsSectionProps {
  /** Array of statistics to display. If not provided, uses default static data */
  statistics?: Statistic[]
  /** Custom title for the section */
  title?: string
  /** Custom subtitle for the section */
  subtitle?: string
  /** Whether to show the section (graceful degradation) */
  visible?: boolean
}

/**
 * Default statistics data for demonstration
 * In a production app, these would come from an API
 */
const defaultStatistics: Statistic[] = [
  {
    id: 'urls-shortened',
    label: 'URLs Shortened',
    value: 1250000,
    icon: <HiLink className="w-8 h-8 text-primary" aria-hidden="true" />,
  },
  {
    id: 'clicks-tracked',
    label: 'Clicks Tracked',
    value: 45700000,
    icon: <HiCursorClick className="w-8 h-8 text-secondary" aria-hidden="true" />,
  },
  {
    id: 'active-users',
    label: 'Active Users',
    value: 52000,
    icon: <HiUsers className="w-8 h-8 text-accent" aria-hidden="true" />,
  },
]

/**
 * StatisticsSection component displays social proof statistics
 * such as URLs shortened, clicks tracked, and user count.
 *
 * The component supports:
 * - Custom statistics data via props
 * - Graceful degradation when statistics are unavailable
 * - Responsive design for mobile/tablet/desktop
 * - Animated number display with Framer Motion
 * - Accessibility features (ARIA labels, reduced motion support)
 */
export default function StatisticsSection({
  statistics = defaultStatistics,
  title = 'Trusted by Thousands',
  subtitle = 'Join our growing community of users who trust us with their links',
  visible = true,
}: StatisticsSectionProps) {
  const shouldReduceMotion = useReducedMotion()

  // Gracefully handle case where section should not be displayed
  if (!visible || !statistics || statistics.length === 0) {
    return null
  }

  const containerAnimation = shouldReduceMotion
    ? {}
    : {
        initial: { opacity: 0 },
        whileInView: { opacity: 1 },
        viewport: { once: true, margin: '-100px' },
        transition: { duration: 0.6 },
      }

  const itemAnimation = (index: number) =>
    shouldReduceMotion
      ? {}
      : {
          initial: { opacity: 0, y: 20 },
          whileInView: { opacity: 1, y: 0 },
          viewport: { once: true },
          transition: { duration: 0.5, delay: index * 0.1 },
        }

  return (
    <section
      id="statistics"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-base-200/50"
      aria-labelledby="statistics-heading"
      data-testid="statistics-section"
    >
      <motion.div className="max-w-7xl mx-auto" {...containerAnimation}>
        {/* Section Header */}
        <div className="text-center mb-12">
          <h2
            id="statistics-heading"
            className="text-3xl sm:text-4xl font-bold mb-4"
            data-testid="statistics-title"
          >
            {title}
          </h2>
          <p
            className="text-base-content/70 text-lg max-w-2xl mx-auto"
            data-testid="statistics-subtitle"
          >
            {subtitle}
          </p>
        </div>

        {/* Statistics Grid */}
        <div
          className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-8"
          data-testid="statistics-grid"
          role="list"
          aria-label="Platform statistics"
        >
          {statistics.map((stat, index) => (
            <motion.div
              key={stat.id}
              className="flex flex-col items-center text-center p-6 rounded-xl bg-base-100 shadow-lg"
              data-testid={`statistic-card-${stat.id}`}
              role="listitem"
              {...itemAnimation(index)}
            >
              {/* Icon */}
              <div
                className="mb-4 p-3 rounded-full bg-base-200"
                data-testid={`statistic-icon-${stat.id}`}
              >
                {stat.icon}
              </div>

              {/* Value */}
              <span
                className="text-4xl sm:text-5xl font-bold bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
                data-testid={`statistic-value-${stat.id}`}
                aria-label={`${stat.value.toLocaleString()} ${stat.label}`}
              >
                {formatNumber(stat.value)}
              </span>

              {/* Label */}
              <span
                className="text-base-content/70 text-lg mt-2 font-medium"
                data-testid={`statistic-label-${stat.id}`}
              >
                {stat.label}
              </span>
            </motion.div>
          ))}
        </div>
      </motion.div>
    </section>
  )
}

export { defaultStatistics }
