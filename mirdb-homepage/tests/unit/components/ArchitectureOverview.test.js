/**
 * Architecture Overview Unit Tests
 * Owner: Scenario 6 - Architecture Overview Section
 *
 * Test coverage:
 * - Renders architecture section with LSM Tree diagram and explanations
 * - LSM Tree diagram shows correct data flow
 * - Memtable explanation describes skip list structure
 * - SSTable explanation describes sorted string tables
 * - Compaction process explanation covers minor and major compaction
 * - Diagram has proper accessibility attributes
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ArchitectureOverview, LSMTreeDiagram } from '../../../src/components/ArchitectureOverview.js'

describe('ArchitectureOverview', () => {
  let container

  beforeEach(() => {
    container = document.createElement('div')
    container.innerHTML = ArchitectureOverview()
    document.body.appendChild(container)
  })

  afterEach(() => {
    document.body.innerHTML = ''
  })

  describe('Test Case 1: Render ArchitectureOverview component', () => {
    it('should render architecture section with LSM Tree diagram and explanations', () => {
      const archSection = container.querySelector('[data-testid="architecture-section"]')
      expect(archSection).toBeTruthy()

      // Should have a diagram
      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')
      expect(diagram).toBeTruthy()

      // Should have explanation sections
      const explanations = container.querySelectorAll('[data-testid^="explanation-"]')
      expect(explanations.length).toBeGreaterThan(0)
    })

    it('should have proper section structure with title', () => {
      const sectionTitle = container.querySelector('.section-title')
      expect(sectionTitle).toBeTruthy()
      expect(sectionTitle.textContent.toLowerCase()).toContain('architecture')
    })

    it('should have semantic structure with heading hierarchy', () => {
      const archSection = container.querySelector('[data-testid="architecture-section"]')
      expect(archSection).toBeTruthy()

      // Should have h2 for section title
      const h2 = archSection.querySelector('h2')
      expect(h2).toBeTruthy()

      // Should have h3 for subsections
      const h3s = archSection.querySelectorAll('h3')
      expect(h3s.length).toBeGreaterThan(0)
    })
  })

  describe('Test Case 2: Check LSM Tree diagram presence', () => {
    it('should display SVG diagram showing LSM Tree structure', () => {
      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')
      expect(diagram).toBeTruthy()

      // Should be an SVG or contain an SVG
      const svg = diagram.tagName.toLowerCase() === 'svg' ? diagram : diagram.querySelector('svg')
      expect(svg).toBeTruthy()
    })

    it('should show Memtable in the diagram', () => {
      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')
      const diagramText = diagram.textContent.toLowerCase()
      expect(diagramText).toContain('memtable')
    })

    it('should show Immutable Memtable in the diagram', () => {
      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')
      const diagramText = diagram.textContent.toLowerCase()
      expect(diagramText).toContain('immutable')
    })

    it('should show Level 0 SSTables in the diagram', () => {
      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')
      const diagramText = diagram.textContent.toLowerCase()
      expect(diagramText).toContain('level 0')
    })

    it('should show Level N SSTables in the diagram', () => {
      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')
      const diagramText = diagram.textContent.toLowerCase()
      // Should show higher levels (Level 1, Level N, etc.)
      expect(diagramText).toMatch(/level\s*[1n]/i)
    })

    it('should show data flow direction in diagram', () => {
      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')
      expect(diagram).toBeTruthy()

      // Should have arrows or flow indicators (path elements)
      const svg = diagram.tagName.toLowerCase() === 'svg' ? diagram : diagram.querySelector('svg')
      const paths = svg.querySelectorAll('path, line, polygon')
      expect(paths.length).toBeGreaterThan(0)
    })
  })

  describe('Test Case 3: Check memtable explanation', () => {
    it('should describe memtable as in-memory structure', () => {
      const memtableSection = container.querySelector('[data-testid="explanation-memtable"]')
      expect(memtableSection).toBeTruthy()

      const text = memtableSection.textContent.toLowerCase()
      expect(text).toContain('in-memory')
    })

    it('should describe skip list structure', () => {
      const memtableSection = container.querySelector('[data-testid="explanation-memtable"]')
      expect(memtableSection).toBeTruthy()

      const text = memtableSection.textContent.toLowerCase()
      expect(text).toContain('skip list')
    })

    it('should mention holding recent writes', () => {
      const memtableSection = container.querySelector('[data-testid="explanation-memtable"]')
      expect(memtableSection).toBeTruthy()

      const text = memtableSection.textContent.toLowerCase()
      expect(text).toMatch(/recent|write|incoming/)
    })
  })

  describe('Test Case 4: Check SSTable explanation', () => {
    it('should describe SSTables as sorted string tables', () => {
      const sstableSection = container.querySelector('[data-testid="explanation-sstable"]')
      expect(sstableSection).toBeTruthy()

      const text = sstableSection.textContent.toLowerCase()
      expect(text).toContain('sorted')
    })

    it('should mention SSTables are on disk', () => {
      const sstableSection = container.querySelector('[data-testid="explanation-sstable"]')
      expect(sstableSection).toBeTruthy()

      const text = sstableSection.textContent.toLowerCase()
      expect(text).toContain('disk')
    })

    it('should describe block structure', () => {
      const sstableSection = container.querySelector('[data-testid="explanation-sstable"]')
      expect(sstableSection).toBeTruthy()

      const text = sstableSection.textContent.toLowerCase()
      expect(text).toContain('block')
    })
  })

  describe('Test Case 5: Check compaction process explanation', () => {
    it('should describe minor compaction (memtable flush)', () => {
      const compactionSection = container.querySelector('[data-testid="explanation-compaction"]')
      expect(compactionSection).toBeTruthy()

      const text = compactionSection.textContent.toLowerCase()
      expect(text).toContain('minor')
      expect(text).toMatch(/flush|memtable/)
    })

    it('should describe major compaction (level merging)', () => {
      const compactionSection = container.querySelector('[data-testid="explanation-compaction"]')
      expect(compactionSection).toBeTruthy()

      const text = compactionSection.textContent.toLowerCase()
      expect(text).toContain('major')
      expect(text).toMatch(/merge|level/)
    })

    it('should explain the purpose of compaction', () => {
      const compactionSection = container.querySelector('[data-testid="explanation-compaction"]')
      expect(compactionSection).toBeTruthy()

      const text = compactionSection.textContent.toLowerCase()
      // Should explain that compaction optimizes storage or read performance
      expect(text).toMatch(/optimiz|performance|reclaim|space|efficien/)
    })
  })

  describe('Test Case 6: Verify diagram accessibility', () => {
    it('should have alt text describing LSM Tree structure', () => {
      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')
      expect(diagram).toBeTruthy()

      // For SVG, check for aria-label, aria-labelledby, or title element
      const svg = diagram.tagName.toLowerCase() === 'svg' ? diagram : diagram.querySelector('svg')
      expect(svg).toBeTruthy()

      const hasAriaLabel = svg.getAttribute('aria-label')
      const hasAriaLabelledBy = svg.getAttribute('aria-labelledby')
      const hasTitle = svg.querySelector('title')
      const hasDesc = svg.querySelector('desc')

      // At least one accessibility attribute should be present
      const hasAccessibility = hasAriaLabel || hasAriaLabelledBy || hasTitle || hasDesc
      expect(hasAccessibility).toBeTruthy()
    })

    it('should have role="img" on SVG diagram', () => {
      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')
      const svg = diagram.tagName.toLowerCase() === 'svg' ? diagram : diagram.querySelector('svg')

      expect(svg.getAttribute('role')).toBe('img')
    })

    it('should mention LSM Tree in accessibility description', () => {
      const diagram = container.querySelector('[data-testid="lsm-tree-diagram"]')
      const svg = diagram.tagName.toLowerCase() === 'svg' ? diagram : diagram.querySelector('svg')

      const ariaLabel = svg.getAttribute('aria-label') || ''
      const title = svg.querySelector('title')?.textContent || ''
      const desc = svg.querySelector('desc')?.textContent || ''

      const accessibleText = (ariaLabel + title + desc).toLowerCase()
      expect(accessibleText).toContain('lsm')
    })
  })
})

describe('LSMTreeDiagram', () => {
  it('should return an SVG element', () => {
    const diagramHtml = LSMTreeDiagram()
    const container = document.createElement('div')
    container.innerHTML = diagramHtml

    const svg = container.querySelector('svg')
    expect(svg).toBeTruthy()
  })

  it('should have proper viewBox for responsive scaling', () => {
    const diagramHtml = LSMTreeDiagram()
    const container = document.createElement('div')
    container.innerHTML = diagramHtml

    const svg = container.querySelector('svg')
    expect(svg.getAttribute('viewBox')).toBeTruthy()
  })

  it('should include all LSM tree components', () => {
    const diagramHtml = LSMTreeDiagram()
    const container = document.createElement('div')
    container.innerHTML = diagramHtml

    const text = container.textContent.toLowerCase()
    expect(text).toContain('memtable')
    expect(text).toContain('sstable')
    expect(text).toContain('level')
  })

  it('should be dark mode compatible with appropriate styling', () => {
    const diagramHtml = LSMTreeDiagram()
    const container = document.createElement('div')
    container.innerHTML = diagramHtml

    const svg = container.querySelector('svg')
    // Should have classes or styles that support dark mode
    const hasClasses = svg.classList.length > 0 || svg.className.baseVal?.length > 0
    const hasCurrentColor = diagramHtml.includes('currentColor')
    const hasDarkModeClasses = diagramHtml.includes('dark:')

    expect(hasClasses || hasCurrentColor || hasDarkModeClasses).toBeTruthy()
  })
})
