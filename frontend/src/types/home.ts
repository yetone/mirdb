/**
 * Homepage Type Definitions
 *
 * Type definitions for homepage components and data structures.
 */

export interface Feature {
  id: number
  title: string
  description: string
  icon: React.ComponentType<{ className?: string }>
}

export interface FAQItem {
  id: number
  question: string
  answer: string
}

export interface HomePageProps {
  className?: string
}
