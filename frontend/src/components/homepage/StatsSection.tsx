/**
 * Statistics Section Component
 * Owner: Scenario 5 - Statistics Display
 *
 * Displays social proof statistics:
 * - Total URLs shortened
 * - Total clicks tracked
 * - Total users (optional)
 *
 * Features:
 * - Animated number counters using Framer Motion
 * - Scroll-triggered animations (Intersection Observer)
 * - Formatted large numbers (e.g., 1.2M)
 */

import { useEffect, useRef, useState, useCallback } from 'react'
import { motion, useMotionValue, useTransform, animate } from 'framer-motion'
import { GlassMorphismCard } from '../GlassMorphismCard'
import type { StatItem } from '../../types/homepage'

export interface StatsSectionProps {
  stats?: StatItem[]
}

const defaultStats: StatItem[] = [
  { label: 'URLs Shortened', value: 1250000, suffix: '+' },
  { label: 'Clicks Tracked', value: 45000000, suffix: '+' },
  { label: 'Active Users', value: 125000, suffix: '+' },
]

/**
 * Formats a number with commas for display
 * e.g., 1234567 -> "1,234,567"
 */
function formatNumber(num: number): string {
  return num.toLocaleString('en-US')
}

/**
 * Formats large numbers with suffix
 * e.g., 1200000 -> "1.2M"
 */
function formatWithSuffix(num: number): string {
  if (num >= 1000000) {
    const millions = num / 1000000
    return millions % 1 === 0 ? `${millions}M` : `${millions.toFixed(1)}M`
  }
  if (num >= 1000) {
    const thousands = num / 1000
    return thousands % 1 === 0 ? `${thousands}K` : `${thousands.toFixed(1)}K`
  }
  return formatNumber(num)
}

/**
 * Converts label to kebab-case for test ID
 */
function toTestId(label: string): string {
  return `stat-${label.toLowerCase().replace(/\s+/g, '-')}`
}

interface AnimatedCounterProps {
  value: number
  suffix?: string
  isVisible: boolean
}

function AnimatedCounter({ value, suffix, isVisible }: AnimatedCounterProps) {
  const motionValue = useMotionValue(0)
  const [displayValue, setDisplayValue] = useState(0)

  useEffect(() => {
    if (isVisible) {
      const controls = animate(motionValue, value, {
        duration: 2,
        ease: 'easeOut',
        onUpdate: (latest) => {
          setDisplayValue(Math.round(latest))
        },
      })

      return () => controls.stop()
    }
  }, [isVisible, value, motionValue])

  return (
    <span className="tabular-nums">
      {formatWithSuffix(displayValue)}
      {suffix && <span className="text-primary">{suffix}</span>}
    </span>
  )
}

export function StatsSection({ stats = defaultStats }: StatsSectionProps) {
  const sectionRef = useRef<HTMLElement>(null)
  const [isVisible, setIsVisible] = useState(false)
  const hasAnimated = useRef(false)

  const handleIntersection = useCallback((entries: IntersectionObserverEntry[]) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting && !hasAnimated.current) {
        setIsVisible(true)
        hasAnimated.current = true
      }
    })
  }, [])

  useEffect(() => {
    const observer = new IntersectionObserver(handleIntersection, {
      threshold: 0.2,
      rootMargin: '0px',
    })

    if (sectionRef.current) {
      observer.observe(sectionRef.current)
    }

    return () => observer.disconnect()
  }, [handleIntersection])

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
    hidden: { opacity: 0, y: 20 },
    visible: {
      opacity: 1,
      y: 0,
      transition: {
        duration: 0.5,
        ease: 'easeOut',
      },
    },
  }

  return (
    <section
      ref={sectionRef}
      id="stats"
      aria-label="Statistics"
      className="py-16 sm:py-24 px-4 sm:px-6 lg:px-8"
      data-testid="stats-section"
    >
      <div className="max-w-6xl mx-auto">
        <motion.h2
          className="text-3xl sm:text-4xl font-bold text-center mb-12"
          initial={{ opacity: 0, y: -20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          Trusted by Thousands
        </motion.h2>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-3 gap-6 lg:gap-8 text-center"
          data-testid="stats-grid"
          variants={containerVariants}
          initial="hidden"
          animate={isVisible ? 'visible' : 'hidden'}
        >
          {stats.map((stat, index) => (
            <motion.div key={index} variants={itemVariants}>
              <GlassMorphismCard
                className="h-full"
              >
                <div
                  className="flex flex-col items-center justify-center py-4"
                  data-testid={toTestId(stat.label)}
                >
                  <span className="text-4xl sm:text-5xl font-bold text-primary mb-2">
                    <AnimatedCounter
                      value={stat.value}
                      suffix={stat.suffix}
                      isVisible={isVisible}
                    />
                  </span>
                  <span className="text-lg text-base-content/70">{stat.label}</span>
                </div>
              </GlassMorphismCard>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  )
}

export default StatsSection
