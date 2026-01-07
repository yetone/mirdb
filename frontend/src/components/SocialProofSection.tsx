import { motion } from 'framer-motion'
import { Link2, MousePointerClick, Users, TrendingUp } from 'lucide-react'

interface Stat {
  icon: React.ReactNode
  value: string
  label: string
}

const stats: Stat[] = [
  {
    icon: <Link2 className="w-6 h-6" />,
    value: '10M+',
    label: 'Links Shortened',
  },
  {
    icon: <MousePointerClick className="w-6 h-6" />,
    value: '500M+',
    label: 'Clicks Tracked',
  },
  {
    icon: <Users className="w-6 h-6" />,
    value: '50K+',
    label: 'Active Users',
  },
  {
    icon: <TrendingUp className="w-6 h-6" />,
    value: '99.9%',
    label: 'Uptime Reliability',
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
  hidden: { opacity: 0, scale: 0.8 },
  visible: {
    opacity: 1,
    scale: 1,
    transition: {
      duration: 0.5,
    },
  },
}

export default function SocialProofSection() {
  return (
    <section
      id="social-proof"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-base-100"
      data-testid="social-proof-section"
    >
      <div className="max-w-7xl mx-auto">
        <div className="text-center mb-12">
          <h2 className="text-3xl sm:text-4xl font-bold text-base-content mb-4">
            Trusted by Thousands
          </h2>
          <p className="text-lg text-base-content/70 max-w-2xl mx-auto">
            Join a growing community of users who rely on our platform every day
          </p>
        </div>

        <motion.div
          className="grid grid-cols-2 md:grid-cols-4 gap-6 lg:gap-8"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-50px' }}
          data-testid="stats-grid"
        >
          {stats.map((stat, index) => (
            <motion.div
              key={index}
              className="card bg-base-200 shadow-lg hover:shadow-xl transition-shadow duration-300"
              variants={itemVariants}
              data-testid={`stat-item-${index}`}
            >
              <div className="card-body items-center text-center py-8">
                <div className="w-12 h-12 rounded-full bg-primary/10 flex items-center justify-center text-primary mb-3">
                  {stat.icon}
                </div>
                <div
                  className="text-3xl sm:text-4xl font-bold text-base-content"
                  data-testid="stat-value"
                >
                  {stat.value}
                </div>
                <div
                  className="text-sm sm:text-base text-base-content/70 font-medium"
                  data-testid="stat-label"
                >
                  {stat.label}
                </div>
              </div>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  )
}
