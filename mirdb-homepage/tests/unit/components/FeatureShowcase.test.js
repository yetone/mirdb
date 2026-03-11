/**
 * Feature Showcase Unit Tests
 * Owner: Scenario 2 - Feature Showcase Section
 *
 * Test coverage:
 * - Renders five feature cards in grid layout
 * - Each feature displays correct title, icon, and description
 * - Responsive grid layout at different viewports
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { FeatureShowcase, FeatureCard } from '../../../src/components/FeatureShowcase.js'

describe('FeatureShowcase', () => {
  let container

  beforeEach(() => {
    container = document.createElement('div')
    container.innerHTML = FeatureShowcase()
    document.body.appendChild(container)
  })

  afterEach(() => {
    document.body.innerHTML = ''
  })

  describe('Test Case 1: Render FeatureShowcase component', () => {
    it('should render five feature cards in grid layout', () => {
      const featureGrid = container.querySelector('[data-testid="feature-grid"]')
      expect(featureGrid).toBeTruthy()

      const featureCards = container.querySelectorAll('[data-testid^="feature-card-"]')
      expect(featureCards.length).toBe(5)

      // Verify grid layout classes
      expect(featureGrid.classList.contains('grid')).toBe(true)
      expect(featureGrid.classList.contains('grid-cols-1')).toBe(true)
      expect(featureGrid.classList.contains('md:grid-cols-2')).toBe(true)
      expect(featureGrid.classList.contains('lg:grid-cols-3')).toBe(true)
    })

    it('should have proper section structure with title and subtitle', () => {
      const sectionTitle = container.querySelector('.section-title')
      expect(sectionTitle).toBeTruthy()
      expect(sectionTitle.textContent.trim()).toBe('Key Features')

      const sectionSubtitle = container.querySelector('.section-subtitle')
      expect(sectionSubtitle).toBeTruthy()
      expect(sectionSubtitle.textContent).toContain('MirDB')
    })

    it('should have accessible role and aria-label on feature grid', () => {
      const featureGrid = container.querySelector('[data-testid="feature-grid"]')
      expect(featureGrid.getAttribute('role')).toBe('list')
      expect(featureGrid.getAttribute('aria-label')).toBe('MirDB Features')
    })
  })

  describe('Test Case 2: Check Memcached Protocol feature', () => {
    it('should display Memcached Protocol feature card with correct content', () => {
      const card = container.querySelector('[data-testid="feature-card-memcached-protocol"]')
      expect(card).toBeTruthy()

      const title = card.querySelector('.feature-title')
      expect(title.textContent.trim()).toBe('Memcached Protocol')

      const description = card.querySelector('.feature-description')
      expect(description.textContent).toContain('memcached text protocol')
      expect(description.textContent).toContain('compatibility')

      const icon = card.querySelector('.feature-icon')
      expect(icon).toBeTruthy()
      expect(icon.tagName.toLowerCase()).toBe('svg')
    })
  })

  describe('Test Case 3: Check Persistent Storage feature', () => {
    it('should display Persistent Storage feature card with correct content', () => {
      const card = container.querySelector('[data-testid="feature-card-persistent-storage"]')
      expect(card).toBeTruthy()

      const title = card.querySelector('.feature-title')
      expect(title.textContent.trim()).toBe('Persistent Storage')

      const description = card.querySelector('.feature-description')
      expect(description.textContent).toContain('SSTable')
      expect(description.textContent).toContain('persists data to disk')

      const icon = card.querySelector('.feature-icon')
      expect(icon).toBeTruthy()
    })
  })

  describe('Test Case 4: Check LSM Tree Architecture feature', () => {
    it('should display LSM Tree Architecture feature card with correct content', () => {
      const card = container.querySelector('[data-testid="feature-card-lsm-tree"]')
      expect(card).toBeTruthy()

      const title = card.querySelector('.feature-title')
      expect(title.textContent.trim()).toBe('LSM Tree Architecture')

      const description = card.querySelector('.feature-description')
      expect(description.textContent).toContain('Log-Structured Merge-tree')
      expect(description.textContent).toContain('memtables')

      const icon = card.querySelector('.feature-icon')
      expect(icon).toBeTruthy()
    })
  })

  describe('Test Case 5: Check Skip List Memtable feature', () => {
    it('should display Skip List Memtable feature card with correct content', () => {
      const card = container.querySelector('[data-testid="feature-card-skip-list"]')
      expect(card).toBeTruthy()

      const title = card.querySelector('.feature-title')
      expect(title.textContent.trim()).toBe('Skip List Memtable')

      const description = card.querySelector('.feature-description')
      expect(description.textContent).toContain('skip list data structure')
      expect(description.textContent).toContain('memtable')

      const icon = card.querySelector('.feature-icon')
      expect(icon).toBeTruthy()
    })
  })

  describe('Test Case 6: Check Multi-level Compaction feature', () => {
    it('should display Multi-level Compaction feature card with correct content', () => {
      const card = container.querySelector('[data-testid="feature-card-compaction"]')
      expect(card).toBeTruthy()

      const title = card.querySelector('.feature-title')
      expect(title.textContent.trim()).toBe('Multi-level Compaction')

      const description = card.querySelector('.feature-description')
      expect(description.textContent).toContain('compaction')
      expect(description.textContent).toContain('SSTable')

      const icon = card.querySelector('.feature-icon')
      expect(icon).toBeTruthy()
    })
  })

  describe('Test Case 7: Test feature grid responsiveness at 768px', () => {
    it('should have responsive grid classes for tablet viewport', () => {
      const featureGrid = container.querySelector('[data-testid="feature-grid"]')

      // Verify responsive Tailwind classes are present
      const gridClasses = featureGrid.className.split(' ')

      // Mobile: 1 column
      expect(gridClasses).toContain('grid-cols-1')

      // Tablet (768px): 2 columns
      expect(gridClasses).toContain('md:grid-cols-2')

      // Desktop: 3 columns
      expect(gridClasses).toContain('lg:grid-cols-3')

      // Verify gap class for spacing
      expect(gridClasses).toContain('gap-6')
    })

    it('should have feature cards that adapt to grid flow', () => {
      const featureCards = container.querySelectorAll('[data-testid^="feature-card-"]')

      featureCards.forEach(card => {
        // Each card should have flex-col for vertical layout
        expect(card.classList.contains('flex')).toBe(true)
        expect(card.classList.contains('flex-col')).toBe(true)
      })
    })
  })
})

describe('FeatureCard', () => {
  it('should render a feature card with provided props', () => {
    const props = {
      id: 'test-feature',
      icon: 'protocol',
      title: 'Test Feature',
      description: 'This is a test feature description.'
    }

    const cardHtml = FeatureCard(props)
    const container = document.createElement('div')
    container.innerHTML = cardHtml

    const card = container.querySelector('[data-testid="feature-card-test-feature"]')
    expect(card).toBeTruthy()

    const title = card.querySelector('.feature-title')
    expect(title.textContent.trim()).toBe('Test Feature')

    const description = card.querySelector('.feature-description')
    expect(description.textContent.trim()).toBe('This is a test feature description.')
  })

  it('should use default icon if provided icon is not found', () => {
    const props = {
      id: 'unknown-icon',
      icon: 'nonexistent',
      title: 'Unknown Icon Feature',
      description: 'Description'
    }

    const cardHtml = FeatureCard(props)
    const container = document.createElement('div')
    container.innerHTML = cardHtml

    const icon = container.querySelector('.feature-icon')
    expect(icon).toBeTruthy()
    expect(icon.tagName.toLowerCase()).toBe('svg')
  })

  it('should include proper semantic markup', () => {
    const props = {
      id: 'semantic-test',
      icon: 'storage',
      title: 'Semantic Test',
      description: 'Testing semantic markup'
    }

    const cardHtml = FeatureCard(props)
    const container = document.createElement('div')
    container.innerHTML = cardHtml

    // Should be an article element
    const card = container.querySelector('article')
    expect(card).toBeTruthy()

    // Title should be h3
    const title = container.querySelector('h3')
    expect(title).toBeTruthy()

    // Description should be a paragraph
    const description = container.querySelector('p')
    expect(description).toBeTruthy()
  })
})
