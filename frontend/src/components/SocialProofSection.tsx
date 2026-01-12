import { motion } from 'framer-motion'

const formatNumber = (num: number): string => {
  if (num >= 1000000) {
    return `${(num / 1000000).toFixed(1).replace(/\.0$/, '')}M+`
  }
  if (num >= 1000) {
    return `${(num / 1000).toFixed(1).replace(/\.0$/, '')}K+`
  }
  return num.toString()
}

const statistics = [
  {
    id: 'urls-shortened',
    value: 1250000,
    label: 'URLs Shortened',
    icon: (
      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-6 h-6">
        <path strokeLinecap="round" strokeLinejoin="round" d="M13.19 8.688a4.5 4.5 0 011.242 7.244l-4.5 4.5a4.5 4.5 0 01-6.364-6.364l1.757-1.757m13.35-.622l1.757-1.757a4.5 4.5 0 00-6.364-6.364l-4.5 4.5a4.5 4.5 0 001.242 7.244" />
      </svg>
    ),
  },
  {
    id: 'clicks-tracked',
    value: 8500000,
    label: 'Clicks Tracked',
    icon: (
      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-6 h-6">
        <path strokeLinecap="round" strokeLinejoin="round" d="M15.042 21.672L13.684 16.6m0 0l-2.51 2.225.569-9.47 5.227 7.917-3.286-.672zM12 2.25V4.5m5.834.166l-1.591 1.591M20.25 10.5H18M7.757 14.743l-1.59 1.59M6 10.5H3.75m4.007-4.243l-1.59-1.59" />
      </svg>
    ),
  },
  {
    id: 'active-users',
    value: 25000,
    label: 'Active Users',
    icon: (
      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-6 h-6">
        <path strokeLinecap="round" strokeLinejoin="round" d="M15 19.128a9.38 9.38 0 002.625.372 9.337 9.337 0 004.121-.952 4.125 4.125 0 00-7.533-2.493M15 19.128v-.003c0-1.113-.285-2.16-.786-3.07M15 19.128v.106A12.318 12.318 0 018.624 21c-2.331 0-4.512-.645-6.374-1.766l-.001-.109a6.375 6.375 0 0111.964-3.07M12 6.375a3.375 3.375 0 11-6.75 0 3.375 3.375 0 016.75 0zm8.25 2.25a2.625 2.625 0 11-5.25 0 2.625 2.625 0 015.25 0z" />
      </svg>
    ),
  },
  {
    id: 'uptime',
    value: 99.9,
    label: 'Uptime',
    suffix: '%',
    icon: (
      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-6 h-6">
        <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75m-3-7.036A11.959 11.959 0 013.598 6 11.99 11.99 0 003 9.749c0 5.592 3.824 10.29 9 11.623 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.571-.598-3.751h-.152c-3.196 0-6.1-1.248-8.25-3.285z" />
      </svg>
    ),
  },
]

const trustIndicators = [
  { id: 'security', text: 'Enterprise-grade Security', icon: '🔒' },
  { id: 'privacy', text: 'GDPR Compliant', icon: '✓' },
  { id: 'support', text: '24/7 Support', icon: '💬' },
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

const SocialProofSection = () => {
  return (
    <section
      id="social-proof"
      data-testid="social-proof-section"
      className="py-20 px-4 bg-base-100"
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
            Trusted by Thousands
          </h2>
          <p className="text-base-content/70 max-w-2xl mx-auto" data-testid="social-proof-description">
            Join the growing community of users who trust us for their URL shortening needs.
          </p>
        </motion.div>

        {/* Statistics Grid */}
        <motion.div
          className="grid grid-cols-2 md:grid-cols-4 gap-6 mb-12"
          data-testid="statistics-container"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
        >
          {statistics.map((stat) => (
            <motion.div
              key={stat.id}
              variants={itemVariants}
              className="card bg-base-200 shadow-lg hover:shadow-xl transition-shadow duration-300"
              data-testid={`statistic-${stat.id}`}
            >
              <div className="card-body items-center text-center p-6">
                <div className="w-12 h-12 rounded-full bg-primary/10 flex items-center justify-center text-primary mb-3">
                  {stat.icon}
                </div>
                <div
                  className="text-3xl md:text-4xl font-bold text-primary"
                  data-testid={`statistic-value-${stat.id}`}
                >
                  {stat.suffix ? `${stat.value}${stat.suffix}` : formatNumber(stat.value)}
                </div>
                <div className="text-sm text-base-content/70 font-medium">
                  {stat.label}
                </div>
              </div>
            </motion.div>
          ))}
        </motion.div>

        {/* Trust Indicators */}
        <motion.div
          className="flex flex-wrap justify-center gap-4 md:gap-8"
          data-testid="trust-indicators"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5, delay: 0.3 }}
        >
          {trustIndicators.map((indicator) => (
            <div
              key={indicator.id}
              className="flex items-center gap-2 px-4 py-2 bg-base-200 rounded-full text-sm font-medium"
              data-testid={`trust-indicator-${indicator.id}`}
            >
              <span className="text-lg">{indicator.icon}</span>
              <span>{indicator.text}</span>
            </div>
          ))}
        </motion.div>
      </div>
    </section>
  )
}

export { formatNumber }
export default SocialProofSection
