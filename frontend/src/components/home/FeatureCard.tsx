/**
 * Individual feature card component.
 * Owner: Scenario 4 - Features Section Display
 * Hover Effects: Scenario 11 - Feature Card Hover Effects
 *
 * Displays a single feature with icon, title, and description.
 * Includes hover effects: subtle lift (transform), border highlight, and shadow elevation.
 */

import { motion } from 'framer-motion'
import { ReactNode } from 'react'

export interface FeatureCardProps {
  icon: ReactNode
  title: string
  description: string
}

export default function FeatureCard({ icon, title, description }: FeatureCardProps) {
  return (
    <motion.article
      className="card bg-base-100 shadow-xl border-2 border-transparent hover:shadow-2xl hover:border-primary transition-all duration-300 ease-out"
      initial={{ opacity: 0, y: 20 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.4 }}
      whileHover={{ y: -4, transition: { duration: 0.2, ease: 'easeOut' } }}
      role="article"
      aria-label={`Feature: ${title}`}
      data-testid="feature-card"
    >
      <div className="card-body items-center text-center">
        <div className="text-primary mb-4" aria-hidden="true">
          {icon}
        </div>
        <h3 className="card-title text-xl font-semibold">{title}</h3>
        <p className="text-base-content/70">{description}</p>
      </div>
    </motion.article>
  )
}
