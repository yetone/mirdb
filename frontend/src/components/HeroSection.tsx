import { Link } from 'react-router-dom'
import { motion } from 'framer-motion'
import { createContext, useContext, useMemo, useSyncExternalStore } from 'react'

// Create a context for reduced motion preference that can be overridden in tests
export const ReducedMotionContext = createContext<boolean | null>(null)

// Hook to detect system reduced motion preference
function useSystemReducedMotion(): boolean {
  const getSnapshot = () => {
    if (typeof window === 'undefined') return false
    return window.matchMedia('(prefers-reduced-motion: reduce)').matches
  }

  const subscribe = (callback: () => void) => {
    if (typeof window === 'undefined') return () => {}
    const mediaQuery = window.matchMedia('(prefers-reduced-motion: reduce)')
    mediaQuery.addEventListener('change', callback)
    return () => mediaQuery.removeEventListener('change', callback)
  }

  return useSyncExternalStore(subscribe, getSnapshot, () => false)
}

// Custom hook that checks context override first, then system preference
function useIsReducedMotion(): boolean {
  const contextValue = useContext(ReducedMotionContext)
  const systemPreference = useSystemReducedMotion()

  return useMemo(() => {
    // Context value takes precedence (for testing)
    if (contextValue !== null) {
      return contextValue
    }
    // Fall back to system preference
    return systemPreference
  }, [contextValue, systemPreference])
}

export default function HeroSection() {
  const prefersReducedMotion = useIsReducedMotion()

  // Animation variants that respect reduced motion preferences
  const fadeInUp = prefersReducedMotion
    ? { initial: {}, animate: {}, transition: {} }
    : {
        initial: { opacity: 0, y: 20 },
        animate: { opacity: 1, y: 0 },
        transition: { duration: 0.6 },
      }

  const fadeInUpDelayed = (delay: number) =>
    prefersReducedMotion
      ? { initial: {}, animate: {}, transition: {} }
      : {
          initial: { opacity: 0, y: 20 },
          animate: { opacity: 1, y: 0 },
          transition: { duration: 0.6, delay },
        }

  return (
    <section
      data-testid="hero-section"
      className="relative min-h-screen flex items-center justify-center bg-gradient-to-br from-primary via-secondary to-accent overflow-hidden"
    >
      {/* Animated background elements - disabled with reduced motion */}
      <div className="absolute inset-0 overflow-hidden">
        <div className="absolute -top-40 -right-40 w-80 h-80 bg-primary/30 rounded-full blur-3xl animate-pulse motion-reduce:animate-none" />
        <div className="absolute -bottom-40 -left-40 w-80 h-80 bg-secondary/30 rounded-full blur-3xl animate-pulse motion-reduce:animate-none delay-1000" />
      </div>

      <div className="relative z-10 text-center px-4 sm:px-6 lg:px-8 max-w-4xl mx-auto">
        <motion.h1
          {...fadeInUp}
          className="text-4xl sm:text-5xl md:text-6xl lg:text-7xl font-bold text-white mb-6"
        >
          <span className="block">Shorten, Share, Track</span>
        </motion.h1>

        <motion.p
          data-testid="hero-subheadline"
          {...fadeInUpDelayed(0.2)}
          className="text-lg sm:text-xl md:text-2xl text-white/90 mb-10 max-w-2xl mx-auto"
        >
          Transform your long URLs into short, memorable links and track every click with powerful analytics.
        </motion.p>

        <motion.div
          {...fadeInUpDelayed(0.4)}
          className="flex flex-col sm:flex-row gap-4 justify-center items-center"
        >
          <Link
            to="/register"
            data-testid="cta-register"
            className="btn btn-lg btn-primary bg-white text-primary hover:bg-white/90 hover:scale-105 hover:shadow-xl border-none shadow-lg transition-all duration-200 min-h-[44px]"
          >
            Get Started Free
          </Link>

          <p className="text-white/80">
            Already have an account?{' '}
            <Link
              to="/login"
              data-testid="login-link"
              className="link link-hover text-white font-semibold underline inline-flex items-center min-h-[44px]"
            >
              Log in
            </Link>
          </p>
        </motion.div>
      </div>
    </section>
  )
}
