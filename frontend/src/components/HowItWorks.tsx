import { motion } from 'framer-motion'
import { UserPlus, Link2, Share2, BarChart3 } from 'lucide-react'

interface Step {
  number: number
  title: string
  description: string
  icon: React.ReactNode
}

const steps: Step[] = [
  {
    number: 1,
    title: 'Create Your Account',
    description: 'Sign up for free in seconds. No credit card required to get started.',
    icon: <UserPlus className="w-8 h-8" />,
  },
  {
    number: 2,
    title: 'Paste Your Long URL',
    description: 'Enter any long URL you want to shorten. We support all valid URLs.',
    icon: <Link2 className="w-8 h-8" />,
  },
  {
    number: 3,
    title: 'Share Your Short Link',
    description: 'Get your shortened link instantly and share it anywhere.',
    icon: <Share2 className="w-8 h-8" />,
  },
  {
    number: 4,
    title: 'Track Performance',
    description: 'Monitor clicks, analyze traffic sources, and track your link analytics.',
    icon: <BarChart3 className="w-8 h-8" />,
  },
]

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.2,
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

export default function HowItWorks() {
  return (
    <section
      id="how-it-works"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-base-200"
      aria-labelledby="how-it-works-heading"
    >
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-12">
          <h2
            id="how-it-works-heading"
            className="text-3xl sm:text-4xl font-bold text-base-content mb-4"
          >
            How It Works
          </h2>
          <p className="text-base-content/70 text-lg max-w-2xl mx-auto">
            Get started in minutes with our simple four-step process
          </p>
        </div>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-100px' }}
          data-testid="how-it-works-steps"
        >
          {steps.map((step) => (
            <motion.div
              key={step.number}
              variants={itemVariants}
              className="relative"
              data-testid={`step-${step.number}`}
            >
              <div className="card bg-base-100 shadow-xl h-full">
                <div className="card-body items-center text-center">
                  <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center text-primary mb-4">
                    {step.icon}
                  </div>
                  <div
                    className="badge badge-primary badge-lg mb-2"
                    aria-label={`Step ${step.number}`}
                  >
                    Step {step.number}
                  </div>
                  <h3 className="card-title text-lg">{step.title}</h3>
                  <p className="text-base-content/70 text-sm">{step.description}</p>
                </div>
              </div>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  )
}
