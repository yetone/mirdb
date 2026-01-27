import { motion } from 'framer-motion'

/**
 * BackgroundEffect component for visual effects.
 * Provides animated gradient background for hero sections.
 */
export function BackgroundEffect() {
  return (
    <div className="absolute inset-0 overflow-hidden -z-10" data-testid="background-effect">
      <motion.div
        className="absolute top-0 left-0 w-96 h-96 bg-primary/20 rounded-full blur-3xl"
        animate={{
          x: [0, 100, 0],
          y: [0, 50, 0],
        }}
        transition={{
          duration: 8,
          repeat: Infinity,
          ease: 'easeInOut',
        }}
      />
      <motion.div
        className="absolute bottom-0 right-0 w-96 h-96 bg-secondary/20 rounded-full blur-3xl"
        animate={{
          x: [0, -100, 0],
          y: [0, -50, 0],
        }}
        transition={{
          duration: 10,
          repeat: Infinity,
          ease: 'easeInOut',
        }}
      />
    </div>
  )
}
