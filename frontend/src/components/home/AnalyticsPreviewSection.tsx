/**
 * AnalyticsPreviewSection - Analytics capability showcase
 * Owner: Scenario 9 - Analytics Preview Section
 *
 * Visual demonstration of analytics features.
 *
 * Expected exports:
 * - AnalyticsPreviewSection: React.FC
 *
 * Features:
 * - Sample chart or statistics visualization
 * - Text explaining analytics benefits
 * - Visual representation of click tracking data
 *
 * Components potentially used:
 * - Recharts for sample charts (existing dependency)
 * - GlassMorphismCard for container
 */
import { motion } from 'framer-motion'
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { ChartBarIcon, GlobeAltIcon, DevicePhoneMobileIcon } from '@heroicons/react/24/outline'
import GlassMorphismCard from '../GlassMorphismCard'

// Sample analytics data for the chart
const sampleChartData = [
  { name: 'Mon', clicks: 120 },
  { name: 'Tue', clicks: 180 },
  { name: 'Wed', clicks: 240 },
  { name: 'Thu', clicks: 200 },
  { name: 'Fri', clicks: 320 },
  { name: 'Sat', clicks: 280 },
  { name: 'Sun', clicks: 350 },
]

// Sample statistics
const sampleStats = [
  {
    label: 'Total Clicks',
    value: '1,690',
    icon: <ChartBarIcon className="w-6 h-6" />,
    description: 'Track every click on your links',
  },
  {
    label: 'Countries',
    value: '24',
    icon: <GlobeAltIcon className="w-6 h-6" />,
    description: 'GeoIP location tracking',
  },
  {
    label: 'Devices',
    value: '3 types',
    icon: <DevicePhoneMobileIcon className="w-6 h-6" />,
    description: 'Browser and device analytics',
  },
]

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.15,
    },
  },
}

const itemVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.5,
    },
  },
}

export function AnalyticsPreviewSection() {
  return (
    <section
      className="py-16 px-4 lg:px-8"
      aria-labelledby="analytics-preview-heading"
      data-testid="analytics-preview-section"
    >
      <div className="max-w-6xl mx-auto">
        {/* Section Header */}
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
          className="text-center mb-12"
        >
          <h2
            id="analytics-preview-heading"
            className="text-3xl lg:text-4xl font-bold mb-4"
          >
            Powerful Analytics at Your Fingertips
          </h2>
          <p
            className="text-base-content/70 max-w-2xl mx-auto"
            data-testid="analytics-description"
          >
            Gain valuable insights into your link performance. Track clicks,
            geographic data, and device information to understand your audience
            better.
          </p>
        </motion.div>

        {/* Main Content Grid */}
        <motion.div
          className="grid grid-cols-1 lg:grid-cols-2 gap-8"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
        >
          {/* Chart Section */}
          <motion.div variants={itemVariants}>
            <GlassMorphismCard className="h-full">
              <h3 className="text-xl font-semibold mb-4">Weekly Click Trends</h3>
              <div
                className="h-64"
                data-testid="analytics-chart"
                role="img"
                aria-label="Sample chart showing weekly click trends"
              >
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={sampleChartData}>
                    <defs>
                      <linearGradient id="clicksGradient" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="hsl(var(--p))" stopOpacity={0.8} />
                        <stop offset="95%" stopColor="hsl(var(--p))" stopOpacity={0.1} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
                    <XAxis
                      dataKey="name"
                      tick={{ fill: 'currentColor' }}
                      axisLine={{ stroke: 'currentColor' }}
                    />
                    <YAxis
                      tick={{ fill: 'currentColor' }}
                      axisLine={{ stroke: 'currentColor' }}
                    />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: 'hsl(var(--b1))',
                        borderColor: 'hsl(var(--b3))',
                        borderRadius: '0.5rem',
                      }}
                    />
                    <Area
                      type="monotone"
                      dataKey="clicks"
                      stroke="hsl(var(--p))"
                      fillOpacity={1}
                      fill="url(#clicksGradient)"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
              <p className="text-sm text-base-content/60 mt-4 italic">
                Sample data showing typical link performance over a week
              </p>
            </GlassMorphismCard>
          </motion.div>

          {/* Statistics Cards */}
          <motion.div variants={itemVariants} className="space-y-4">
            {sampleStats.map((stat, index) => (
              <GlassMorphismCard key={index} className="flex items-center gap-4">
                <div
                  className="w-12 h-12 rounded-full bg-primary/10 text-primary flex items-center justify-center flex-shrink-0"
                  data-testid={`stat-icon-${index}`}
                >
                  {stat.icon}
                </div>
                <div className="flex-grow">
                  <div className="flex items-baseline gap-2">
                    <span
                      className="text-2xl font-bold text-primary"
                      data-testid={`stat-value-${index}`}
                    >
                      {stat.value}
                    </span>
                    <span className="text-base-content/70">{stat.label}</span>
                  </div>
                  <p className="text-sm text-base-content/60">{stat.description}</p>
                </div>
              </GlassMorphismCard>
            ))}
          </motion.div>
        </motion.div>

        {/* Benefits Section */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5, delay: 0.3 }}
          className="mt-12 text-center"
        >
          <GlassMorphismCard>
            <h3 className="text-xl font-semibold mb-3">
              Why Analytics Matter
            </h3>
            <p
              className="text-base-content/70 max-w-3xl mx-auto"
              data-testid="analytics-benefits"
            >
              Understanding who clicks your links and when helps you optimize your
              marketing campaigns, identify your most engaged audiences, and make
              data-driven decisions. Our comprehensive analytics give you the
              insights you need to grow smarter.
            </p>
          </GlassMorphismCard>
        </motion.div>
      </div>
    </section>
  )
}

export default AnalyticsPreviewSection
