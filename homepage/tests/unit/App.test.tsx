/**
 * Unit tests for App component
 * Owner: Scenario 1 - Homepage Layout & Structure
 *
 * Tests verify that all required sections are rendered in the correct order
 * as per the PRD layout structure.
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import App from '../../src/App'

describe('App Component - Homepage Layout & Structure', () => {
  /**
   * Test Case 1: Navbar component is rendered at the top of the page
   */
  it('should render Navbar component at the top of the page', () => {
    render(<App />)

    const navbar = screen.getByTestId('navbar')
    expect(navbar).toBeInTheDocument()

    // Verify navbar is within a nav element
    expect(navbar.tagName.toLowerCase()).toBe('nav')
  })

  /**
   * Test Case 2: Hero section component is rendered below navbar
   */
  it('should render Hero section component below navbar', () => {
    render(<App />)

    const hero = screen.getByTestId('hero')
    expect(hero).toBeInTheDocument()

    // Verify hero is a section element
    expect(hero.tagName.toLowerCase()).toBe('section')
  })

  /**
   * Test Case 3: Features section component is rendered
   */
  it('should render Features section component', () => {
    render(<App />)

    const features = screen.getByTestId('features')
    expect(features).toBeInTheDocument()

    // Verify features is a section element
    expect(features.tagName.toLowerCase()).toBe('section')
  })

  /**
   * Test Case 4: QuickStart section component is rendered
   */
  it('should render QuickStart section component', () => {
    render(<App />)

    const quickstart = screen.getByTestId('quickstart')
    expect(quickstart).toBeInTheDocument()

    // Verify quickstart is a section element
    expect(quickstart.tagName.toLowerCase()).toBe('section')
  })

  /**
   * Test Case 5: Status/Badges section component is rendered
   */
  it('should render Status/Badges section component', () => {
    render(<App />)

    const statusBadges = screen.getByTestId('status-badges')
    expect(statusBadges).toBeInTheDocument()

    // Verify status-badges is a section element
    expect(statusBadges.tagName.toLowerCase()).toBe('section')
  })

  /**
   * Test Case 6: Footer component is rendered at the bottom
   */
  it('should render Footer component at the bottom', () => {
    render(<App />)

    const footer = screen.getByTestId('footer')
    expect(footer).toBeInTheDocument()

    // Verify footer is a footer element
    expect(footer.tagName.toLowerCase()).toBe('footer')
  })

  /**
   * Additional test: Verify sections appear in correct order
   */
  it('should render all sections in the correct vertical order', () => {
    const { container } = render(<App />)

    // Get all sections and layout elements in DOM order
    const navbar = container.querySelector('[data-testid="navbar"]')
    const hero = container.querySelector('[data-testid="hero"]')
    const features = container.querySelector('[data-testid="features"]')
    const quickstart = container.querySelector('[data-testid="quickstart"]')
    const statusBadges = container.querySelector('[data-testid="status-badges"]')
    const footer = container.querySelector('[data-testid="footer"]')

    // Verify all sections exist
    expect(navbar).not.toBeNull()
    expect(hero).not.toBeNull()
    expect(features).not.toBeNull()
    expect(quickstart).not.toBeNull()
    expect(statusBadges).not.toBeNull()
    expect(footer).not.toBeNull()

    // Get positions using compareDocumentPosition
    // Node.DOCUMENT_POSITION_FOLLOWING (4) means the reference node follows the compared node
    const FOLLOWING = Node.DOCUMENT_POSITION_FOLLOWING

    // Verify order: navbar < hero < features < quickstart < statusBadges < footer
    expect(navbar!.compareDocumentPosition(hero!)).toBe(FOLLOWING)
    expect(hero!.compareDocumentPosition(features!)).toBe(FOLLOWING)
    expect(features!.compareDocumentPosition(quickstart!)).toBe(FOLLOWING)
    expect(quickstart!.compareDocumentPosition(statusBadges!)).toBe(FOLLOWING)
    expect(statusBadges!.compareDocumentPosition(footer!)).toBe(FOLLOWING)
  })
})
