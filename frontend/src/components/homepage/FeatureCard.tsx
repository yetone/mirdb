/**
 * Individual feature card component.
 * Owner: Scenario 3 - Features Section Display
 *
 * Displays a single feature with icon, title, and description
 * using GlassMorphism styling.
 */

import React from 'react'
import GlassMorphismCard from '@/components/GlassMorphismCard'
import { FeatureCardProps } from '@/types/homepage'

const FeatureCard: React.FC<FeatureCardProps> = ({ icon, title, description }) => {
  return (
    <GlassMorphismCard className="h-full hover:scale-105 transition-transform duration-300">
      <div className="flex flex-col items-center text-center space-y-4">
        <div className="w-16 h-16 flex items-center justify-center rounded-full bg-primary/20 text-primary">
          {icon}
        </div>
        <h3 className="text-xl font-bold text-base-content">{title}</h3>
        <p className="text-base-content/80 leading-relaxed">{description}</p>
      </div>
    </GlassMorphismCard>
  )
}

export default FeatureCard
