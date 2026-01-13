import { Link2, MousePointerClick, Users, Globe } from 'lucide-react'
import { motion } from 'framer-motion'
import { formatStatNumber } from '../utils/formatNumber'

interface Statistic {
  id: string
  label: string
  value: number
  icon: React.ReactNode
}

interface SocialProofSectionProps {
  urlsShortened?: number
  clicksTracked?: number
  activeUsers?: number
  countriesReached?: number
}

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.15,
    },
  },
}

const statVariants = {
  hidden: { opacity: 0, scale: 0.8 },
  visible: {
    opacity: 1,
    scale: 1,
    transition: {
      duration: 0.5,
      ease: 'easeOut',
    },
  },
}

export function SocialProofSection({
  urlsShortened = 1250000,
  clicksTracked = 45600000,
  activeUsers = 12500,
  countriesReached = 150,
}: SocialProofSectionProps) {
  const statistics: Statistic[] = [
    {
      id: 'urls-shortened',
      label: 'URLs Shortened',
      value: urlsShortened,
      icon: <Link2 className="w-8 h-8" aria-hidden="true" />,
    },
    {
      id: 'clicks-tracked',
      label: 'Clicks Tracked',
      value: clicksTracked,
      icon: <MousePointerClick className="w-8 h-8" aria-hidden="true" />,
    },
    {
      id: 'active-users',
      label: 'Active Users',
      value: activeUsers,
      icon: <Users className="w-8 h-8" aria-hidden="true" />,
    },
    {
      id: 'countries-reached',
      label: 'Countries Reached',
      value: countriesReached,
      icon: <Globe className="w-8 h-8" aria-hidden="true" />,
    },
  ]

  return (
    <section
      id="social-proof"
      data-testid="social-proof-section"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-base-300"
    >
      <div className="max-w-7xl mx-auto">
        <div className="text-center mb-12">
          <h2 className="text-3xl sm:text-4xl font-bold text-base-content mb-4">
            Trusted by Thousands
          </h2>
          <p className="text-lg text-base-content max-w-2xl mx-auto">
            Join our growing community of users who trust us with their links
          </p>
        </div>

        <motion.div
          data-testid="statistics-container"
          className="grid grid-cols-2 md:grid-cols-4 gap-6 md:gap-8"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-50px' }}
        >
          {statistics.map((stat) => (
            <motion.div
              key={stat.id}
              data-testid={`stat-card-${stat.id}`}
              className="text-center p-6 rounded-xl bg-base-100/50 backdrop-blur-sm border border-base-content/10 shadow-lg"
              variants={statVariants}
            >
              <div
                data-testid={`stat-icon-${stat.id}`}
                className="w-14 h-14 mx-auto rounded-full bg-primary/20 flex items-center justify-center text-primary mb-4"
              >
                {stat.icon}
              </div>
              <div
                data-testid={`stat-value-${stat.id}`}
                className="text-3xl md:text-4xl font-bold text-base-content mb-2"
              >
                {formatStatNumber(stat.value)}
              </div>
              <div
                data-testid={`stat-label-${stat.id}`}
                className="text-sm md:text-base text-base-content font-medium"
              >
                {stat.label}
              </div>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  )
}

export default SocialProofSection
