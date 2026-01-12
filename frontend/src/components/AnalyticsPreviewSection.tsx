import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'

const analyticsFeatures = [
  {
    id: 'clicks',
    title: 'Real-time Click Tracking',
    description: 'Monitor every click as it happens with live updates.',
    icon: (
      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-6 h-6">
        <path strokeLinecap="round" strokeLinejoin="round" d="M15.042 21.672L13.684 16.6m0 0l-2.51 2.225.569-9.47 5.227 7.917-3.286-.672zM12 2.25V4.5m5.834.166l-1.591 1.591M20.25 10.5H18M7.757 14.743l-1.59 1.59M6 10.5H3.75m4.007-4.243l-1.59-1.59" />
      </svg>
    ),
  },
  {
    id: 'geo',
    title: 'Geographic Insights',
    description: 'See where your audience is located worldwide.',
    icon: (
      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-6 h-6">
        <path strokeLinecap="round" strokeLinejoin="round" d="M12 21a9.004 9.004 0 008.716-6.747M12 21a9.004 9.004 0 01-8.716-6.747M12 21c2.485 0 4.5-4.03 4.5-9S14.485 3 12 3m0 18c-2.485 0-4.5-4.03-4.5-9S9.515 3 12 3m0 0a8.997 8.997 0 017.843 4.582M12 3a8.997 8.997 0 00-7.843 4.582m15.686 0A11.953 11.953 0 0112 10.5c-2.998 0-5.74-1.1-7.843-2.918m15.686 0A8.959 8.959 0 0121 12c0 .778-.099 1.533-.284 2.253m0 0A17.919 17.919 0 0112 16.5c-3.162 0-6.133-.815-8.716-2.247m0 0A9.015 9.015 0 013 12c0-1.605.42-3.113 1.157-4.418" />
      </svg>
    ),
  },
  {
    id: 'browser',
    title: 'Browser & Device Stats',
    description: 'Know what devices and browsers your visitors use.',
    icon: (
      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-6 h-6">
        <path strokeLinecap="round" strokeLinejoin="round" d="M9 17.25v1.007a3 3 0 01-.879 2.122L7.5 21h9l-.621-.621A3 3 0 0115 18.257V17.25m6-12V15a2.25 2.25 0 01-2.25 2.25H5.25A2.25 2.25 0 013 15V5.25m18 0A2.25 2.25 0 0018.75 3H5.25A2.25 2.25 0 003 5.25m18 0V12a2.25 2.25 0 01-2.25 2.25H5.25A2.25 2.25 0 013 12V5.25" />
      </svg>
    ),
  },
  {
    id: 'referrers',
    title: 'Referrer Tracking',
    description: 'Discover where your traffic is coming from.',
    icon: (
      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-6 h-6">
        <path strokeLinecap="round" strokeLinejoin="round" d="M13.19 8.688a4.5 4.5 0 011.242 7.244l-4.5 4.5a4.5 4.5 0 01-6.364-6.364l1.757-1.757m13.35-.622l1.757-1.757a4.5 4.5 0 00-6.364-6.364l-4.5 4.5a4.5 4.5 0 001.242 7.244" />
      </svg>
    ),
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
  hidden: { opacity: 0, x: -20 },
  visible: {
    opacity: 1,
    x: 0,
    transition: {
      duration: 0.5,
    },
  },
}

const AnalyticsPreviewSection = () => {
  return (
    <section
      id="analytics-preview"
      data-testid="analytics-preview-section"
      className="py-20 px-4 bg-base-200"
    >
      <div className="container mx-auto">
        <motion.div
          className="text-center mb-12"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          <h2 className="text-3xl md:text-4xl font-bold mb-4">
            Powerful Analytics Dashboard
          </h2>
          <p data-testid="analytics-description" className="text-base-content/70 max-w-2xl mx-auto">
            Track every click with detailed insights. Monitor your link performance in real-time
            and understand your audience with comprehensive analytics.
          </p>
        </motion.div>

        <div className="flex flex-col lg:flex-row items-center gap-12">
          {/* Dashboard Preview Mockup */}
          <motion.div
            className="flex-1 w-full"
            initial={{ opacity: 0, x: -50 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.6 }}
          >
            <div
              data-testid="analytics-dashboard-preview"
              className="relative bg-base-100 rounded-xl shadow-2xl p-6 overflow-hidden"
            >
              {/* Dashboard Mockup SVG */}
              <svg
                viewBox="0 0 500 300"
                className="w-full h-auto"
                aria-label="Analytics dashboard preview"
              >
                {/* Background */}
                <rect x="0" y="0" width="500" height="300" fill="currentColor" className="text-base-100" />

                {/* Header bar */}
                <rect x="0" y="0" width="500" height="40" fill="currentColor" className="text-base-200" />
                <circle cx="20" cy="20" r="6" fill="currentColor" className="text-error" />
                <circle cx="40" cy="20" r="6" fill="currentColor" className="text-warning" />
                <circle cx="60" cy="20" r="6" fill="currentColor" className="text-success" />
                <rect x="100" y="15" width="100" height="10" rx="2" fill="currentColor" className="text-base-content/30" />

                {/* Stats cards */}
                <rect x="20" y="55" width="100" height="60" rx="8" fill="currentColor" className="text-primary/10" />
                <rect x="30" y="65" width="60" height="8" rx="2" fill="currentColor" className="text-base-content/50" />
                <motion.text
                  x="30"
                  y="100"
                  fill="currentColor"
                  className="text-primary text-xl font-bold"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ delay: 0.5 }}
                >
                  12,847
                </motion.text>

                <rect x="135" y="55" width="100" height="60" rx="8" fill="currentColor" className="text-secondary/10" />
                <rect x="145" y="65" width="60" height="8" rx="2" fill="currentColor" className="text-base-content/50" />
                <motion.text
                  x="145"
                  y="100"
                  fill="currentColor"
                  className="text-secondary text-xl font-bold"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ delay: 0.7 }}
                >
                  48 URLs
                </motion.text>

                <rect x="250" y="55" width="100" height="60" rx="8" fill="currentColor" className="text-accent/10" />
                <rect x="260" y="65" width="60" height="8" rx="2" fill="currentColor" className="text-base-content/50" />
                <motion.text
                  x="260"
                  y="100"
                  fill="currentColor"
                  className="text-accent text-xl font-bold"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ delay: 0.9 }}
                >
                  32 Countries
                </motion.text>

                {/* Chart area background */}
                <rect x="20" y="130" width="330" height="150" rx="8" fill="currentColor" className="text-base-200" />

                {/* Chart grid lines */}
                <line x1="40" y1="150" x2="330" y2="150" stroke="currentColor" className="text-base-content/10" strokeWidth="1" />
                <line x1="40" y1="180" x2="330" y2="180" stroke="currentColor" className="text-base-content/10" strokeWidth="1" />
                <line x1="40" y1="210" x2="330" y2="210" stroke="currentColor" className="text-base-content/10" strokeWidth="1" />
                <line x1="40" y1="240" x2="330" y2="240" stroke="currentColor" className="text-base-content/10" strokeWidth="1" />

                {/* Animated chart bars */}
                <motion.rect
                  x="50"
                  y="260"
                  width="25"
                  height="0"
                  rx="2"
                  fill="currentColor"
                  className="text-primary"
                  animate={{ height: 80, y: 180 }}
                  transition={{ duration: 0.6, delay: 0.3 }}
                />
                <motion.rect
                  x="85"
                  y="260"
                  width="25"
                  height="0"
                  rx="2"
                  fill="currentColor"
                  className="text-primary"
                  animate={{ height: 60, y: 200 }}
                  transition={{ duration: 0.6, delay: 0.4 }}
                />
                <motion.rect
                  x="120"
                  y="260"
                  width="25"
                  height="0"
                  rx="2"
                  fill="currentColor"
                  className="text-primary"
                  animate={{ height: 100, y: 160 }}
                  transition={{ duration: 0.6, delay: 0.5 }}
                />
                <motion.rect
                  x="155"
                  y="260"
                  width="25"
                  height="0"
                  rx="2"
                  fill="currentColor"
                  className="text-primary"
                  animate={{ height: 70, y: 190 }}
                  transition={{ duration: 0.6, delay: 0.6 }}
                />
                <motion.rect
                  x="190"
                  y="260"
                  width="25"
                  height="0"
                  rx="2"
                  fill="currentColor"
                  className="text-primary"
                  animate={{ height: 90, y: 170 }}
                  transition={{ duration: 0.6, delay: 0.7 }}
                />
                <motion.rect
                  x="225"
                  y="260"
                  width="25"
                  height="0"
                  rx="2"
                  fill="currentColor"
                  className="text-primary"
                  animate={{ height: 110, y: 150 }}
                  transition={{ duration: 0.6, delay: 0.8 }}
                />
                <motion.rect
                  x="260"
                  y="260"
                  width="25"
                  height="0"
                  rx="2"
                  fill="currentColor"
                  className="text-primary"
                  animate={{ height: 85, y: 175 }}
                  transition={{ duration: 0.6, delay: 0.9 }}
                />
                <motion.rect
                  x="295"
                  y="260"
                  width="25"
                  height="0"
                  rx="2"
                  fill="currentColor"
                  className="text-primary"
                  animate={{ height: 95, y: 165 }}
                  transition={{ duration: 0.6, delay: 1.0 }}
                />

                {/* Pie chart sidebar */}
                <rect x="365" y="130" width="120" height="150" rx="8" fill="currentColor" className="text-base-200" />
                <motion.circle
                  cx="425"
                  cy="190"
                  r="40"
                  fill="none"
                  stroke="currentColor"
                  className="text-primary"
                  strokeWidth="20"
                  strokeDasharray="126 251"
                  initial={{ strokeDashoffset: 251 }}
                  animate={{ strokeDashoffset: 0 }}
                  transition={{ duration: 1, delay: 0.5 }}
                />
                <motion.circle
                  cx="425"
                  cy="190"
                  r="40"
                  fill="none"
                  stroke="currentColor"
                  className="text-secondary"
                  strokeWidth="20"
                  strokeDasharray="75 251"
                  strokeDashoffset="-126"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ duration: 0.5, delay: 1 }}
                />
                <motion.circle
                  cx="425"
                  cy="190"
                  r="40"
                  fill="none"
                  stroke="currentColor"
                  className="text-accent"
                  strokeWidth="20"
                  strokeDasharray="50 251"
                  strokeDashoffset="-201"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ duration: 0.5, delay: 1.2 }}
                />

                {/* Legend items */}
                <rect x="375" y="245" width="8" height="8" rx="2" fill="currentColor" className="text-primary" />
                <rect x="388" y="245" width="30" height="8" rx="2" fill="currentColor" className="text-base-content/30" />
                <rect x="430" y="245" width="8" height="8" rx="2" fill="currentColor" className="text-secondary" />
                <rect x="443" y="245" width="30" height="8" rx="2" fill="currentColor" className="text-base-content/30" />
              </svg>

              {/* Decorative gradient overlay */}
              <div className="absolute inset-0 bg-gradient-to-t from-base-100/50 to-transparent pointer-events-none" />
            </div>
          </motion.div>

          {/* Feature Highlights */}
          <motion.div
            className="flex-1"
            variants={containerVariants}
            initial="hidden"
            whileInView="visible"
            viewport={{ once: true }}
          >
            <div data-testid="analytics-feature-highlights" className="space-y-6">
              {analyticsFeatures.map((feature) => (
                <motion.div
                  key={feature.id}
                  data-testid={`analytics-feature-${feature.id}`}
                  variants={itemVariants}
                  className="flex items-start gap-4 p-4 rounded-lg bg-base-100 shadow-md hover:shadow-lg transition-shadow"
                >
                  <div className="w-12 h-12 rounded-full bg-primary/10 flex items-center justify-center text-primary flex-shrink-0">
                    {feature.icon}
                  </div>
                  <div>
                    <h3 className="font-semibold text-lg mb-1">{feature.title}</h3>
                    <p className="text-base-content/70 text-sm">{feature.description}</p>
                  </div>
                </motion.div>
              ))}
            </div>

            {/* CTA Button */}
            <motion.div
              className="mt-8"
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ duration: 0.5, delay: 0.6 }}
            >
              <Link
                to="/register"
                data-testid="analytics-cta"
                className="btn btn-primary btn-lg w-full sm:w-auto"
              >
                See Your Analytics
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor" className="w-5 h-5 ml-2">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 4.5L21 12m0 0l-7.5 7.5M21 12H3" />
                </svg>
              </Link>
            </motion.div>
          </motion.div>
        </div>
      </div>
    </section>
  )
}

export default AnalyticsPreviewSection
