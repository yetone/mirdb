import { motion } from 'framer-motion'
import { Link2, BarChart3, LayoutDashboard, Shield } from 'lucide-react'

interface Feature {
  icon: React.ReactNode
  title: string
  description: string
}

const features: Feature[] = [
  {
    icon: <Link2 className="w-8 h-8" />,
    title: 'Lightning-Fast URL Shortening',
    description: 'Transform long, unwieldy URLs into clean, shareable short links in milliseconds. Perfect for social media, marketing campaigns, and everyday sharing.',
  },
  {
    icon: <BarChart3 className="w-8 h-8" />,
    title: 'Comprehensive Click Analytics',
    description: 'Track every click with detailed analytics. Monitor geographic data, referral sources, device types, and engagement patterns in real-time.',
  },
  {
    icon: <LayoutDashboard className="w-8 h-8" />,
    title: 'Easy Link Management Dashboard',
    description: 'Organize and manage all your short links from one intuitive dashboard. Edit, archive, or delete links with ease.',
  },
  {
    icon: <Shield className="w-8 h-8" />,
    title: 'Secure Authentication & Privacy',
    description: 'Your links are protected with enterprise-grade security. JWT-based authentication ensures your data stays private and secure.',
  },
]

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.1,
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

export default function FeaturesSection() {
  return (
    <section
      id="features"
      className="py-20 px-4 sm:px-6 lg:px-8 bg-base-200"
      data-testid="features-section"
    >
      <div className="max-w-7xl mx-auto">
        <div className="text-center mb-16">
          <h2 className="text-3xl sm:text-4xl font-bold text-base-content mb-4">
            Powerful Features
          </h2>
          <p className="text-lg text-base-content/70 max-w-2xl mx-auto">
            Everything you need to create, manage, and analyze your shortened URLs
          </p>
        </div>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-100px' }}
          data-testid="features-grid"
        >
          {features.map((feature, index) => (
            <motion.div
              key={index}
              className="card bg-base-100 shadow-xl hover:shadow-2xl transition-shadow duration-300"
              variants={itemVariants}
              data-testid="feature-card"
            >
              <div className="card-body items-center text-center">
                <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center text-primary mb-4">
                  {feature.icon}
                </div>
                <h3 className="card-title text-lg" data-testid="feature-title">
                  {feature.title}
                </h3>
                <p className="text-base-content/70" data-testid="feature-description">
                  {feature.description}
                </p>
              </div>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  )
}
