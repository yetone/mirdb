import { motion } from 'framer-motion'
import FuturisticButton from './FuturisticButton'
import BackgroundEffect from './BackgroundEffect'

interface HeroSectionProps {
  'data-testid'?: string
}

export default function HeroSection({ 'data-testid': testId }: HeroSectionProps) {
  return (
    <section
      data-testid={testId || 'hero-section'}
      className="relative min-h-screen flex items-center justify-center overflow-hidden"
    >
      <BackgroundEffect data-testid="hero-background" />

      <div className="relative z-10 container mx-auto px-4 py-16 text-center">
        <motion.h1
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6 text-base-content"
          data-testid="hero-headline"
        >
          Shorten. Share. Analyze.
        </motion.h1>

        <motion.p
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
          className="text-lg md:text-xl lg:text-2xl text-base-content/80 mb-10 max-w-2xl mx-auto"
          data-testid="hero-subheadline"
        >
          Transform your long URLs into powerful short links. Track clicks, analyze performance, and share with confidence.
        </motion.p>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.4 }}
          className="flex flex-col sm:flex-row gap-4 justify-center items-center"
        >
          <FuturisticButton
            as="link"
            to="/register"
            variant="primary"
            size="lg"
            data-testid="cta-get-started"
          >
            Get Started Free
          </FuturisticButton>

          <FuturisticButton
            as="link"
            to="/login"
            variant="secondary"
            size="lg"
            data-testid="cta-login"
          >
            Login
          </FuturisticButton>
        </motion.div>

        <motion.nav
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.6, delay: 0.6 }}
          className="mt-12 flex gap-6 justify-center text-base-content/60"
          aria-label="Page sections"
        >
          <a
            href="#features"
            className="hover:text-primary transition-colors"
            data-testid="nav-features"
          >
            Features
          </a>
          <a
            href="#demo"
            className="hover:text-primary transition-colors"
            data-testid="nav-demo"
          >
            Demo
          </a>
        </motion.nav>
      </div>
    </section>
  )
}
