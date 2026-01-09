import { motion, useReducedMotion } from 'framer-motion'
import FuturisticButton from './FuturisticButton'

export interface HeroSectionProps {
  headline?: string
  subheadline?: string
  primaryCtaText?: string
  primaryCtaLink?: string
  secondaryCtaText?: string
  secondaryCtaLink?: string
}

const defaultProps: Required<HeroSectionProps> = {
  headline: 'Shorten. Share. Track.',
  subheadline:
    'Create short, memorable URLs and track every click with powerful analytics.',
  primaryCtaText: 'Get Started Free',
  primaryCtaLink: '/register',
  secondaryCtaText: 'Log In',
  secondaryCtaLink: '/login',
}

export function HeroSection(props: HeroSectionProps) {
  const {
    headline = defaultProps.headline,
    subheadline = defaultProps.subheadline,
    primaryCtaText = defaultProps.primaryCtaText,
    primaryCtaLink = defaultProps.primaryCtaLink,
    secondaryCtaText = defaultProps.secondaryCtaText,
    secondaryCtaLink = defaultProps.secondaryCtaLink,
  } = props

  const shouldReduceMotion = useReducedMotion()

  const animationProps = shouldReduceMotion
    ? { initial: { opacity: 1, y: 0 }, animate: { opacity: 1, y: 0 }, transition: { duration: 0 } }
    : { initial: { opacity: 0, y: 20 }, animate: { opacity: 1, y: 0 } }

  return (
    <section
      data-testid="hero-section"
      className="hero min-h-[80vh] bg-gradient-to-br from-base-200 via-base-100 to-base-200"
    >
      <div className="hero-content text-center">
        <motion.div
          className="max-w-2xl"
          {...animationProps}
          transition={shouldReduceMotion ? { duration: 0 } : { duration: 0.6 }}
          data-reduced-motion={shouldReduceMotion ? 'true' : 'false'}
        >
          <motion.h1
            data-testid="hero-headline"
            className="text-5xl md:text-6xl font-bold bg-gradient-to-r from-primary via-secondary to-accent bg-clip-text text-transparent mb-6"
            {...animationProps}
            transition={shouldReduceMotion ? { duration: 0 } : { duration: 0.6, delay: 0.2 }}
          >
            {headline}
          </motion.h1>
          <motion.p
            data-testid="hero-subheadline"
            className="text-lg md:text-xl text-base-content/80 mb-8"
            {...animationProps}
            transition={shouldReduceMotion ? { duration: 0 } : { duration: 0.6, delay: 0.4 }}
          >
            {subheadline}
          </motion.p>
          <motion.div
            className="flex flex-col sm:flex-row gap-4 justify-center items-center"
            {...animationProps}
            transition={shouldReduceMotion ? { duration: 0 } : { duration: 0.6, delay: 0.6 }}
          >
            <FuturisticButton
              as="link"
              to={primaryCtaLink}
              variant="primary"
              size="lg"
              data-testid="hero-cta-primary"
            >
              {primaryCtaText}
            </FuturisticButton>
            <FuturisticButton
              as="link"
              to={secondaryCtaLink}
              variant="outline"
              size="lg"
              data-testid="hero-cta-secondary"
            >
              {secondaryCtaText}
            </FuturisticButton>
          </motion.div>
        </motion.div>
      </div>
    </section>
  )
}

export default HeroSection
