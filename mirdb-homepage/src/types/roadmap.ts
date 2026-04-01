/**
 * Roadmap-related type definitions.
 * Owner: Scenario 4 - Roadmap Section
 */

export type RoadmapStatus = 'complete' | 'in-progress' | 'planned'

export interface RoadmapItem {
  id: string
  title: string
  description?: string
  status: RoadmapStatus
}
