/**
 * Individual feature card component.
 * Owner: Scenario 4 - Features Section Display
 *
 * Displays a single feature with icon, title, and description.
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
      className="card bg-base-100 shadow-xl hover:shadow-2xl transition-shadow duration-300"
      initial={{ opacity: 0, y: 20 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.4 }}
      whileHover={{ y: -4, transition: { duration: 0.2 } }}
      role="article"
      aria-label={`Feature: ${title}`}
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
