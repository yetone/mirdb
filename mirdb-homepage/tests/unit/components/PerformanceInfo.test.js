/**
 * Performance Information Unit Tests
 * Owner: Scenario 7 - Performance Information Section
 *
 * Test coverage:
 * - Performance section with benchmarks and configuration options displayed
 * - Write/read performance characteristics or benchmark results
 * - memtable_max_size option with default 4MB and tuning guidance
 * - sstable_max_size option with default 100MB and tuning guidance
 * - max_levels option with default 7 and impact explanation
 * - Tuning guidelines for each configuration option
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { PerformanceInfo, ConfigOption, PerformanceCharacteristics } from '../../../src/components/PerformanceInfo.js'

describe('PerformanceInfo', () => {
  let container

  beforeEach(() => {
    container = document.createElement('div')
    container.innerHTML = PerformanceInfo()
    document.body.appendChild(container)
  })

  afterEach(() => {
    document.body.innerHTML = ''
  })

  describe('Test Case 1: Render PerformanceInfo component', () => {
    it('should render performance section with benchmarks and configuration options displayed', () => {
      // Check main section structure
      const sectionTitle = container.querySelector('.section-title')
      expect(sectionTitle).toBeTruthy()
      expect(sectionTitle.textContent).toContain('Performance')
      expect(sectionTitle.textContent).toContain('Configuration')

      // Check performance characteristics section
      const perfCharacteristics = container.querySelector('[data-testid="performance-characteristics"]')
      expect(perfCharacteristics).toBeTruthy()

      // Check configuration grid
      const configGrid = container.querySelector('[data-testid="config-grid"]')
      expect(configGrid).toBeTruthy()

      // Verify there are configuration options displayed
      const configOptions = container.querySelectorAll('[data-testid^="config-option-"]')
      expect(configOptions.length).toBeGreaterThanOrEqual(3)
    })

    it('should have proper section structure with title and subtitle', () => {
      const sectionTitle = container.querySelector('.section-title')
      expect(sectionTitle).toBeTruthy()

      const sectionSubtitle = container.querySelector('.section-subtitle')
      expect(sectionSubtitle).toBeTruthy()
      expect(sectionSubtitle.textContent).toContain('LSM tree')
    })

    it('should have accessible role and aria-label on config grid', () => {
      const configGrid = container.querySelector('[data-testid="config-grid"]')
      expect(configGrid.getAttribute('role')).toBe('list')
      expect(configGrid.getAttribute('aria-label')).toBe('MirDB Configuration Options')
    })
  })

  describe('Test Case 2: Check performance characteristics', () => {
    it('should display write/read performance characteristics or benchmark results', () => {
      const perfCharacteristics = container.querySelector('[data-testid="performance-characteristics"]')
      expect(perfCharacteristics).toBeTruthy()

      // Check for write performance metric
      const writeMetric = container.querySelector('[data-testid="perf-metric-writes"]')
      expect(writeMetric).toBeTruthy()
      expect(writeMetric.textContent).toContain('Write Performance')

      // Check for read performance metric
      const readMetric = container.querySelector('[data-testid="perf-metric-reads"]')
      expect(readMetric).toBeTruthy()
      expect(readMetric.textContent).toContain('Read Performance')

      // Check for durability metric
      const durabilityMetric = container.querySelector('[data-testid="perf-metric-durability"]')
      expect(durabilityMetric).toBeTruthy()
      expect(durabilityMetric.textContent).toContain('Durability')
    })

    it('should include performance descriptions for each metric', () => {
      const writeMetric = container.querySelector('[data-testid="perf-metric-writes"]')
      expect(writeMetric.textContent).toContain('memtable')

      const readMetric = container.querySelector('[data-testid="perf-metric-reads"]')
      expect(readMetric.textContent).toContain('Bloom filters')

      const durabilityMetric = container.querySelector('[data-testid="perf-metric-durability"]')
      expect(durabilityMetric.textContent).toContain('SSTable')
    })
  })

  describe('Test Case 3: Check memtable size config option', () => {
    it('should show memtable_max_size option with default 4MB and tuning guidance', () => {
      const memtableOption = container.querySelector('[data-testid="config-option-memtable-max-size"]')
      expect(memtableOption).toBeTruthy()

      // Check parameter name
      const configName = memtableOption.querySelector('.config-name')
      expect(configName.textContent.trim()).toBe('memtable_max_size')

      // Check default value
      const configDefault = container.querySelector('[data-testid="config-default-memtable-max-size"]')
      expect(configDefault).toBeTruthy()
      expect(configDefault.textContent).toContain('4MB')

      // Check description
      const configDescription = memtableOption.querySelector('.config-description')
      expect(configDescription.textContent).toContain('memtable')

      // Check tuning guidance
      const tuningText = memtableOption.querySelector('.tuning-text')
      expect(tuningText).toBeTruthy()
      expect(tuningText.textContent.length).toBeGreaterThan(10)
    })
  })

  describe('Test Case 4: Check SSTable size config option', () => {
    it('should show sstable_max_size option with default 100MB and tuning guidance', () => {
      const sstableOption = container.querySelector('[data-testid="config-option-sstable-max-size"]')
      expect(sstableOption).toBeTruthy()

      // Check parameter name
      const configName = sstableOption.querySelector('.config-name')
      expect(configName.textContent.trim()).toBe('sstable_max_size')

      // Check default value
      const configDefault = container.querySelector('[data-testid="config-default-sstable-max-size"]')
      expect(configDefault).toBeTruthy()
      expect(configDefault.textContent).toContain('100MB')

      // Check description
      const configDescription = sstableOption.querySelector('.config-description')
      expect(configDescription.textContent).toContain('SSTable')

      // Check tuning guidance
      const tuningText = sstableOption.querySelector('.tuning-text')
      expect(tuningText).toBeTruthy()
      expect(tuningText.textContent.length).toBeGreaterThan(10)
    })
  })

  describe('Test Case 5: Check LSM levels config option', () => {
    it('should show max_levels option with default 7 and impact explanation', () => {
      const levelsOption = container.querySelector('[data-testid="config-option-max-levels"]')
      expect(levelsOption).toBeTruthy()

      // Check parameter name
      const configName = levelsOption.querySelector('.config-name')
      expect(configName.textContent.trim()).toBe('max_levels')

      // Check default value
      const configDefault = container.querySelector('[data-testid="config-default-max-levels"]')
      expect(configDefault).toBeTruthy()
      expect(configDefault.textContent).toContain('7')

      // Check description includes impact explanation
      const configDescription = levelsOption.querySelector('.config-description')
      expect(configDescription.textContent).toContain('LSM tree')

      // Check tuning guidance with impact
      const tuningText = levelsOption.querySelector('.tuning-text')
      expect(tuningText).toBeTruthy()
      expect(tuningText.textContent).toMatch(/level|amplification|compaction/i)
    })
  })

  describe('Test Case 6: Check tuning guidelines', () => {
    it('should provide guidance on when to adjust each configuration option', () => {
      // Check tuning summary section exists
      const tuningSummary = container.querySelector('[data-testid="tuning-summary"]')
      expect(tuningSummary).toBeTruthy()

      // Check for specific tuning guidelines
      const guidelines = container.querySelector('.tuning-guidelines')
      expect(guidelines).toBeTruthy()

      // Check for workload-specific guidance
      expect(tuningSummary.textContent).toContain('Write-heavy')
      expect(tuningSummary.textContent).toContain('Read-heavy')

      // Each config option should have tuning guidance
      const configOptions = container.querySelectorAll('[data-testid^="config-option-"]')
      configOptions.forEach(option => {
        const tuningSection = option.querySelector('.config-tuning')
        expect(tuningSection).toBeTruthy()

        const tuningLabel = option.querySelector('.tuning-label')
        expect(tuningLabel.textContent).toContain('Tuning')

        const tuningText = option.querySelector('.tuning-text')
        expect(tuningText.textContent.length).toBeGreaterThan(10)
      })
    })

    it('should provide general tuning guidelines for different workload types', () => {
      const tuningSummary = container.querySelector('[data-testid="tuning-summary"]')

      // Check for guidance on different scenarios
      expect(tuningSummary.textContent).toContain('Large datasets')
      expect(tuningSummary.textContent).toContain('Memory-constrained')
    })
  })

  describe('Additional tests for component robustness', () => {
    it('should have proper semantic markup for accessibility', () => {
      // Config options should be article elements
      const configArticles = container.querySelectorAll('[data-testid^="config-option-"]')
      configArticles.forEach(article => {
        expect(article.tagName.toLowerCase()).toBe('article')
      })

      // Should have proper heading hierarchy
      const h2 = container.querySelector('h2')
      expect(h2).toBeTruthy()

      const h3s = container.querySelectorAll('h3')
      expect(h3s.length).toBeGreaterThan(0)
    })

    it('should include icons for each configuration option', () => {
      const configOptions = container.querySelectorAll('[data-testid^="config-option-"]')
      configOptions.forEach(option => {
        const icon = option.querySelector('.config-icon')
        expect(icon).toBeTruthy()
        expect(icon.tagName.toLowerCase()).toBe('svg')
      })
    })

    it('should have dark mode compatible classes', () => {
      const darkClasses = container.innerHTML.match(/dark:/g)
      expect(darkClasses).toBeTruthy()
      expect(darkClasses.length).toBeGreaterThan(5)
    })
  })
})

describe('ConfigOption', () => {
  it('should render a config option card with provided props', () => {
    const props = {
      id: 'test-config',
      name: 'test_parameter',
      default: '10MB',
      description: 'This is a test parameter description.',
      tuning: 'Adjust based on your needs.',
      impact: 'write_performance'
    }

    const cardHtml = ConfigOption(props)
    const container = document.createElement('div')
    container.innerHTML = cardHtml

    const card = container.querySelector('[data-testid="config-option-test-config"]')
    expect(card).toBeTruthy()

    const name = card.querySelector('.config-name')
    expect(name.textContent.trim()).toBe('test_parameter')

    const defaultBadge = container.querySelector('[data-testid="config-default-test-config"]')
    expect(defaultBadge.textContent).toContain('10MB')

    const description = card.querySelector('.config-description')
    expect(description.textContent.trim()).toBe('This is a test parameter description.')

    const tuning = card.querySelector('.tuning-text')
    expect(tuning.textContent.trim()).toBe('Adjust based on your needs.')
  })

  it('should use default icon if impact type is unknown', () => {
    const props = {
      id: 'unknown-impact',
      name: 'unknown_param',
      default: '5',
      description: 'Description',
      tuning: 'Tuning guidance',
      impact: 'nonexistent_impact'
    }

    const cardHtml = ConfigOption(props)
    const container = document.createElement('div')
    container.innerHTML = cardHtml

    const icon = container.querySelector('.config-icon')
    expect(icon).toBeTruthy()
    expect(icon.tagName.toLowerCase()).toBe('svg')
  })
})

describe('PerformanceCharacteristics', () => {
  it('should render all three performance metrics', () => {
    const html = PerformanceCharacteristics()
    const container = document.createElement('div')
    container.innerHTML = html

    const metrics = container.querySelectorAll('.perf-metric')
    expect(metrics.length).toBe(3)

    expect(container.textContent).toContain('Write Performance')
    expect(container.textContent).toContain('Read Performance')
    expect(container.textContent).toContain('Durability')
  })

  it('should have proper grid layout for responsiveness', () => {
    const html = PerformanceCharacteristics()
    const container = document.createElement('div')
    container.innerHTML = html

    const grid = container.querySelector('.grid')
    expect(grid).toBeTruthy()
    expect(grid.classList.contains('grid-cols-1')).toBe(true)
    expect(grid.classList.contains('md:grid-cols-3')).toBe(true)
  })
})
