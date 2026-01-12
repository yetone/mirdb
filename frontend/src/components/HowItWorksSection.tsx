import { motion } from 'framer-motion'

const steps = [
  {
    number: 1,
    title: 'Paste Your URL',
    description: 'Enter your long URL into our shortener. Any valid link works - from blog posts to product pages.',
    icon: (
      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-8 h-8">
        <path strokeLinecap="round" strokeLinejoin="round" d="M15.666 3.888A2.25 2.25 0 0013.5 2.25h-3c-1.03 0-1.9.693-2.166 1.638m7.332 0c.055.194.084.4.084.612v0a.75.75 0 01-.75.75H9.75a.75.75 0 01-.75-.75v0c0-.212.03-.418.084-.612m7.332 0c.646.049 1.288.11 1.927.184 1.1.128 1.907 1.077 1.907 2.185V19.5a2.25 2.25 0 01-2.25 2.25H6.75A2.25 2.25 0 014.5 19.5V6.257c0-1.108.806-2.057 1.907-2.185a48.208 48.208 0 011.927-.184" />
      </svg>
    ),
  },
  {
    number: 2,
    title: 'Get Your Short Link',
    description: 'Instantly receive your shortened link. Copy it with one click and share it anywhere you like.',
    icon: (
      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-8 h-8">
        <path strokeLinecap="round" strokeLinejoin="round" d="M13.19 8.688a4.5 4.5 0 011.242 7.244l-4.5 4.5a4.5 4.5 0 01-6.364-6.364l1.757-1.757m13.35-.622l1.757-1.757a4.5 4.5 0 00-6.364-6.364l-4.5 4.5a4.5 4.5 0 001.242 7.244" />
      </svg>
    ),
  },
  {
    number: 3,
    title: 'Track Clicks & Analyze',
    description: 'Monitor your link performance with detailed analytics. Track clicks, locations, and more.',
    icon: (
      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-8 h-8">
        <path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z" />
      </svg>
    ),
  },
]

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.3,
    },
  },
}

const itemVariants = {
  hidden: { opacity: 0, y: 30 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.6,
    },
  },
}

const HowItWorksSection = () => {
  return (
    <section
      id="how-it-works"
      data-testid="how-it-works-section"
      className="py-20 px-4 bg-base-100"
    >
      <div className="container mx-auto">
        <motion.div
          className="text-center mb-16"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          <h2 className="text-3xl md:text-4xl font-bold mb-4">
            How It Works
          </h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Get started in three simple steps. No account required to try it out.
          </p>
        </motion.div>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-3 gap-8 max-w-5xl mx-auto"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
        >
          {steps.map((step) => (
            <motion.div
              key={step.number}
              data-testid={`step-${step.number}`}
              variants={itemVariants}
              className="relative flex flex-col items-center text-center"
            >
              {/* Step number badge */}
              <div className="w-12 h-12 rounded-full bg-primary text-primary-content flex items-center justify-center text-xl font-bold mb-4">
                {step.number}
              </div>

              {/* Icon */}
              <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center text-primary mb-4">
                {step.icon}
              </div>

              {/* Content */}
              <h3 className="text-xl font-semibold mb-2">{step.title}</h3>
              <p className="text-base-content/70">{step.description}</p>

              {/* Connector line for larger screens (except last item) */}
              {step.number < 3 && (
                <div className="hidden md:block absolute top-6 left-[60%] w-[80%] h-0.5 bg-primary/20" />
              )}
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  )
}

export default HowItWorksSection
