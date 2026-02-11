/**
 * Feature card component for displaying individual features.
 * Owner: Scenario 4 - Features Section
 *
 * Requirements:
 * - REQ-7: Feature name and description
 * - US-5: Technical benefit explanation
 */

import React from 'react'
import './FeatureCard.css'

interface FeatureCardProps {
  name: string
  description: string
  icon?: React.ReactNode
}

export function FeatureCard({ name, description, icon }: FeatureCardProps) {
  return (
    <article className="feature-card">
      {icon && <div className="feature-card__icon">{icon}</div>}
      <h3 className="feature-card__title">{name}</h3>
      <p className="feature-card__description">{description}</p>
    </article>
  )
}
