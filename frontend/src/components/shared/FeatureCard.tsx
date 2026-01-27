/**
 * Reusable Feature Card Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Purpose: Generic feature card for displaying product features.
 */

import React from 'react'

interface FeatureCardProps {
  icon: React.ReactNode
  title: string
  description: string
}

export const FeatureCard: React.FC<FeatureCardProps> = ({
  icon,
  title,
  description,
}) => {
  return (
    <article className="backdrop-blur-md bg-base-100/70 border border-base-content/10 rounded-xl shadow-xl p-6 transition-all duration-300 hover:shadow-2xl hover:bg-base-100/80 hover:scale-105 hover:-translate-y-1">
      <div className="flex flex-col items-center text-center gap-4">
        <div className="w-12 h-12 flex items-center justify-center text-primary">
          {icon}
        </div>
        <h3 className="text-xl font-semibold text-base-content">{title}</h3>
        <p className="text-base-content/70">{description}</p>
      </div>
    </article>
  )
}
