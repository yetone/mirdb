import { motion } from 'framer-motion'
import { useContext } from 'react'
import { ReducedMotionContext } from './HeroSection'

interface Step {
  number: number
  title: string
  description: string
  icon: React.ReactNode
}

// Hook to check reduced motion preference
function useIsReducedMotion(): boolean {
  const contextValue = useContext(ReducedMotionContext)
  if (contextValue !== null) {
    return contextValue
  }
  if (typeof window === 'undefined') return false
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches
}

const ClipboardIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    fill="none"
    viewBox="0 0 24 24"
    strokeWidth={1.5}
    stroke="currentColor"
    className="w-8 h-8"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M15.666 3.888A2.25 2.25 0 0 0 13.5 2.25h-3c-1.03 0-1.9.693-2.166 1.638m7.332 0c.055.194.084.4.084.612v0a.75.75 0 0 1-.75.75H9.75a.75.75 0 0 1-.75-.75v0c0-.212.03-.418.084-.612m7.332 0c.646.049 1.288.11 1.927.184 1.1.128 1.907 1.077 1.907 2.185V19.5a2.25 2.25 0 0 1-2.25 2.25H6.75A2.25 2.25 0 0 1 4.5 19.5V6.257c0-1.108.806-2.057 1.907-2.185a48.208 48.208 0 0 1 1.927-.184"
    />
  </svg>
)

const LinkIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    fill="none"
    viewBox="0 0 24 24"
    strokeWidth={1.5}
    stroke="currentColor"
    className="w-8 h-8"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M13.19 8.688a4.5 4.5 0 0 1 1.242 7.244l-4.5 4.5a4.5 4.5 0 0 1-6.364-6.364l1.757-1.757m13.35-.622 1.757-1.757a4.5 4.5 0 0 0-6.364-6.364l-4.5 4.5a4.5 4.5 0 0 0 1.242 7.244"
    />
  </svg>
)

const ChartIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    fill="none"
    viewBox="0 0 24 24"
    strokeWidth={1.5}
    stroke="currentColor"
    className="w-8 h-8"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 0 1 3 19.875v-6.75ZM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V8.625ZM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V4.125Z"
    />
  </svg>
)

const steps: Step[] = [
  {
    number: 1,
    title: 'Paste Your Long URL',
    description: 'Copy your long URL and paste it into our simple input field.',
    icon: <ClipboardIcon />,
  },
  {
    number: 2,
    title: 'Get Your Shortened Link',
    description: 'Instantly receive a compact, memorable short link.',
    icon: <LinkIcon />,
  },
  {
    number: 3,
    title: 'Share and Track Performance',
    description: 'Share your link and monitor clicks, locations, and referrers.',
    icon: <ChartIcon />,
  },
]

function StepCard({ step }: { step: Step }) {
  return (
    <div
      data-testid={`step-card-${step.number}`}
      className="card bg-base-100 shadow-xl hover:shadow-2xl transition-shadow duration-300"
    >
      <div className="card-body items-center text-center">
        <div
          data-testid={`step-number-${step.number}`}
          className="flex items-center justify-center w-12 h-12 rounded-full bg-primary text-primary-content font-bold text-xl mb-4"
        >
          {step.number}
        </div>
        <div
          data-testid={`step-icon-${step.number}`}
          className="text-primary mb-4"
        >
          {step.icon}
        </div>
        <h3 className="card-title text-lg">{step.title}</h3>
        <p className="text-base-content/70">{step.description}</p>
      </div>
    </div>
  )
}

function HowItWorksSection() {
  const prefersReducedMotion = useIsReducedMotion()

  // Animation variants for section header
  const headerVariants = prefersReducedMotion
    ? {}
    : {
        initial: { opacity: 0, y: 30 },
        whileInView: { opacity: 1, y: 0 },
        transition: { duration: 0.6, ease: 'easeOut' },
        viewport: { once: true, amount: 0.3 },
      }

  // Animation variants for step cards with stagger effect
  const cardVariants = (index: number) =>
    prefersReducedMotion
      ? {}
      : {
          initial: { opacity: 0, y: 40 },
          whileInView: { opacity: 1, y: 0 },
          transition: { duration: 0.5, delay: index * 0.15, ease: 'easeOut' },
          viewport: { once: true, amount: 0.2 },
        }

  return (
    <section
      data-testid="how-it-works-section"
      className="py-16 px-4 bg-base-200"
    >
      <div className="container mx-auto max-w-6xl">
        <motion.h2
          className="text-3xl md:text-4xl font-bold text-center mb-12"
          data-testid="how-it-works-heading"
          {...headerVariants}
        >
          How It Works
        </motion.h2>
        <div
          data-testid="steps-container"
          className="grid grid-cols-1 md:grid-cols-3 gap-8"
        >
          {steps.map((step, index) => (
            <motion.div key={step.number} {...cardVariants(index)}>
              <StepCard step={step} />
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  )
}

export default HowItWorksSection
