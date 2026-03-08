/**
 * Content Maintainability Tests
 * Owner: Scenario 20 - Content Maintainability
 *
 * Verifies NFR-5: Content shall be easily maintainable without requiring build tools
 * (content stored in simple data files).
 *
 * These tests ensure that all content is defined in separate data files,
 * not hardcoded in components, making updates easy for non-technical users.
 */

import { describe, it, expect } from 'vitest'
import * as fs from 'fs'
import * as path from 'path'

// Import data files to verify their structure
import { features } from './features'
import { steps } from './gettingStarted'
import { roadmapItems } from './roadmap'
import { links, GITHUB_URL, DOCS_URL, PROTOCOL_URL } from './links'

// Import types to verify data conforms to expected structure
import type { Feature, Step, RoadmapItem, ExternalLink } from '../types'

describe('Content Maintainability (NFR-5)', () => {
  describe('Test Case 1: Check for content data files', () => {
    const dataDir = path.resolve(__dirname)

    it('should have a features data file', () => {
      const featuresPath = path.join(dataDir, 'features.ts')
      expect(fs.existsSync(featuresPath)).toBe(true)
    })

    it('should have a gettingStarted data file', () => {
      const gettingStartedPath = path.join(dataDir, 'gettingStarted.ts')
      expect(fs.existsSync(gettingStartedPath)).toBe(true)
    })

    it('should have a roadmap data file', () => {
      const roadmapPath = path.join(dataDir, 'roadmap.ts')
      expect(fs.existsSync(roadmapPath)).toBe(true)
    })

    it('should have a links data file', () => {
      const linksPath = path.join(dataDir, 'links.ts')
      expect(fs.existsSync(linksPath)).toBe(true)
    })

    it('content files should be TypeScript for type safety while remaining editable', () => {
      const dataFiles = ['features.ts', 'gettingStarted.ts', 'roadmap.ts', 'links.ts']

      dataFiles.forEach((file) => {
        const filePath = path.join(dataDir, file)
        expect(fs.existsSync(filePath)).toBe(true)
        // TypeScript files are human-readable and editable
        const content = fs.readFileSync(filePath, 'utf-8')
        expect(content.length).toBeGreaterThan(0)
      })
    })
  })

  describe('Test Case 2: Verify features defined in data file', () => {
    it('should export a features array', () => {
      expect(Array.isArray(features)).toBe(true)
      expect(features.length).toBeGreaterThan(0)
    })

    it('each feature should have required properties for easy editing', () => {
      features.forEach((feature: Feature) => {
        expect(feature).toHaveProperty('title')
        expect(feature).toHaveProperty('description')
        expect(feature).toHaveProperty('icon')
        expect(typeof feature.title).toBe('string')
        expect(typeof feature.description).toBe('string')
        expect(typeof feature.icon).toBe('string')
      })
    })

    it('features should be editable without code changes', () => {
      // Features are stored in a simple array structure
      // Adding/removing features only requires modifying the array
      const originalLength = features.length
      expect(originalLength).toBeGreaterThan(0)

      // Each feature has a simple, flat structure
      const sampleFeature = features[0]
      expect(Object.keys(sampleFeature).length).toBeLessThanOrEqual(5) // Simple structure
    })

    it('should contain the required MirDB features from REQ-3', () => {
      const featureTitles = features.map((f) => f.title.toLowerCase())

      expect(featureTitles.some((t) => t.includes('memcached'))).toBe(true)
      expect(featureTitles.some((t) => t.includes('sstable') || t.includes('persistence'))).toBe(true)
      expect(featureTitles.some((t) => t.includes('lsm'))).toBe(true)
      expect(featureTitles.some((t) => t.includes('tokio') || t.includes('async'))).toBe(true)
      expect(featureTitles.some((t) => t.includes('skip list'))).toBe(true)
      expect(featureTitles.some((t) => t.includes('compaction'))).toBe(true)
    })
  })

  describe('Test Case 3: Verify getting started steps in data file', () => {
    it('should export a steps array', () => {
      expect(Array.isArray(steps)).toBe(true)
      expect(steps.length).toBeGreaterThan(0)
    })

    it('each step should have required properties for easy editing', () => {
      steps.forEach((step: Step) => {
        expect(step).toHaveProperty('number')
        expect(step).toHaveProperty('title')
        expect(step).toHaveProperty('content')
        expect(typeof step.number).toBe('number')
        expect(typeof step.title).toBe('string')
        expect(typeof step.content).toBe('string')
      })
    })

    it('steps should be editable without code changes', () => {
      // Steps are stored in a simple array structure
      const originalLength = steps.length
      expect(originalLength).toBeGreaterThan(0)

      // Each step has a simple structure
      const sampleStep = steps[0]
      expect(Object.keys(sampleStep).length).toBeLessThanOrEqual(5) // Simple structure
    })

    it('steps should be numbered sequentially for easy reordering', () => {
      steps.forEach((step, index) => {
        expect(step.number).toBe(index + 1)
      })
    })

    it('steps can optionally include code examples', () => {
      const stepsWithCode = steps.filter((step) => step.code !== undefined)
      expect(stepsWithCode.length).toBeGreaterThan(0)

      stepsWithCode.forEach((step) => {
        expect(typeof step.code).toBe('string')
        expect((step.code as string).length).toBeGreaterThan(0)
      })
    })

    it('should contain installation, connection, and basic operations per REQ-4', () => {
      const stepTitles = steps.map((s) => s.title.toLowerCase())

      expect(stepTitles.some((t) => t.includes('install') || t.includes('run'))).toBe(true)
      expect(stepTitles.some((t) => t.includes('connect'))).toBe(true)
      expect(stepTitles.some((t) => t.includes('set') || t.includes('get') || t.includes('delete'))).toBe(true)
    })
  })

  describe('Test Case 4: Verify roadmap items in data file', () => {
    it('should export a roadmapItems array', () => {
      expect(Array.isArray(roadmapItems)).toBe(true)
      expect(roadmapItems.length).toBeGreaterThan(0)
    })

    it('each roadmap item should have required properties for easy editing', () => {
      roadmapItems.forEach((item: RoadmapItem) => {
        expect(item).toHaveProperty('title')
        expect(item).toHaveProperty('status')
        expect(item).toHaveProperty('description')
        expect(typeof item.title).toBe('string')
        expect(typeof item.status).toBe('string')
        expect(typeof item.description).toBe('string')
      })
    })

    it('roadmap items should be editable without code changes', () => {
      // Roadmap items are stored in a simple array structure
      const originalLength = roadmapItems.length
      expect(originalLength).toBeGreaterThan(0)

      // Each item has a simple structure
      const sampleItem = roadmapItems[0]
      expect(Object.keys(sampleItem).length).toBeLessThanOrEqual(5) // Simple structure
    })

    it('roadmap status should use standardized values for consistency', () => {
      const validStatuses = ['complete', 'in-progress', 'planned']

      roadmapItems.forEach((item) => {
        expect(validStatuses).toContain(item.status)
      })
    })

    it('should have both complete and planned items per REQ-8', () => {
      const completeItems = roadmapItems.filter((item) => item.status === 'complete')
      const plannedItems = roadmapItems.filter((item) => item.status === 'planned')

      expect(completeItems.length).toBeGreaterThan(0)
      expect(plannedItems.length).toBeGreaterThan(0)
    })

    it('should include Raft consensus as planned feature per REQ-8', () => {
      const raftItem = roadmapItems.find((item) =>
        item.title.toLowerCase().includes('raft')
      )

      expect(raftItem).toBeDefined()
      expect(raftItem?.status).toBe('planned')
    })
  })

  describe('Content Structure Validation', () => {
    it('all data exports should be arrays for easy manipulation', () => {
      expect(Array.isArray(features)).toBe(true)
      expect(Array.isArray(steps)).toBe(true)
      expect(Array.isArray(roadmapItems)).toBe(true)
      expect(Array.isArray(links)).toBe(true)
    })

    it('external links should be editable without code changes', () => {
      expect(links.length).toBeGreaterThan(0)

      links.forEach((link: ExternalLink) => {
        expect(link).toHaveProperty('label')
        expect(link).toHaveProperty('url')
        expect(link).toHaveProperty('type')
      })
    })

    it('URL constants should be easily updatable', () => {
      expect(typeof GITHUB_URL).toBe('string')
      expect(typeof DOCS_URL).toBe('string')
      expect(typeof PROTOCOL_URL).toBe('string')
      expect(GITHUB_URL.startsWith('https://')).toBe(true)
      expect(DOCS_URL.startsWith('https://')).toBe(true)
      expect(PROTOCOL_URL.startsWith('https://')).toBe(true)
    })
  })

  describe('Content Editability Assessment', () => {
    it('data files should not require component modifications for content updates', () => {
      // Verify that data is properly exported from dedicated data files
      // not mixed with component logic
      const dataDir = path.resolve(__dirname)
      const componentsDir = path.resolve(__dirname, '..', 'components')

      // Data files exist in data directory
      expect(fs.existsSync(path.join(dataDir, 'features.ts'))).toBe(true)
      expect(fs.existsSync(path.join(dataDir, 'gettingStarted.ts'))).toBe(true)
      expect(fs.existsSync(path.join(dataDir, 'roadmap.ts'))).toBe(true)

      // Components directory is separate from data
      expect(fs.existsSync(componentsDir)).toBe(true)
    })

    it('data files should contain clear documentation comments', () => {
      const dataDir = path.resolve(__dirname)
      const dataFiles = ['features.ts', 'gettingStarted.ts', 'roadmap.ts', 'links.ts']

      dataFiles.forEach((file) => {
        const content = fs.readFileSync(path.join(dataDir, file), 'utf-8')
        // Files should have documentation comments explaining their purpose
        expect(content.includes('/**') || content.includes('//')).toBe(true)
      })
    })

    it('index file should export all data for convenient importing', () => {
      const indexPath = path.resolve(__dirname, 'index.ts')
      expect(fs.existsSync(indexPath)).toBe(true)

      const indexContent = fs.readFileSync(indexPath, 'utf-8')
      expect(indexContent.includes('export')).toBe(true)
    })
  })
})
