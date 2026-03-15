import React from 'react'
import {
  Network,
  HardDrive,
  Layers,
  Combine,
  Shield,
  Zap,
  type LucideIcon,
} from 'lucide-react'

export interface FeatureCardProps {
  title: string
  description: string
  icon: string
}

const iconMap: Record<string, LucideIcon> = {
  Network,
  HardDrive,
  Layers,
  Combine,
  Shield,
  Zap,
}

export default function FeatureCard({ title, description, icon }: FeatureCardProps) {
  const IconComponent = iconMap[icon] || Shield

  return (
    <div
      className="relative p-6 bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700 hover:shadow-md transition-shadow"
      data-testid="feature-card"
    >
      <div
        className="w-12 h-12 flex items-center justify-center rounded-lg bg-rust-100 dark:bg-rust-900/30 text-rust-600 dark:text-rust-400 mb-4"
        data-testid="feature-icon"
        aria-hidden="true"
      >
        <IconComponent className="w-6 h-6" />
      </div>
      <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
        {title}
      </h3>
      <p className="text-gray-600 dark:text-gray-300 text-sm leading-relaxed">
        {description}
      </p>
    </div>
  )
}
