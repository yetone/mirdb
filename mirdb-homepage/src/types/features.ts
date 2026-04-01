/**
 * Feature-related type definitions.
 * Owner: Scenario 3 - Features Section
 */

export type FeatureStatus = 'complete' | 'in-progress' | 'planned'

export interface Feature {
  id: string
  title: string
  description: string
  icon: string
  status: FeatureStatus
}
