import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import AnalyticsPreviewSection from './AnalyticsPreviewSection'
import Register from '../pages/Register'

// Mock scrollIntoView
Element.prototype.scrollIntoView = vi.fn()

const renderWithRouter = (initialEntries = ['/']) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <Routes>
        <Route path="/" element={<AnalyticsPreviewSection />} />
        <Route path="/register" element={<Register />} />
      </Routes>
    </MemoryRouter>
  )
}

describe('AnalyticsPreviewSection - Unit Tests', () => {
  // Test Case 1: Section exists with dashboard preview image or mockup
  it('renders analytics preview section with dashboard mockup', () => {
    renderWithRouter()

    // Verify section exists
    const section = screen.getByTestId('analytics-preview-section')
    expect(section).toBeInTheDocument()

    // Verify dashboard preview/mockup exists
    const dashboardPreview = screen.getByTestId('analytics-dashboard-preview')
    expect(dashboardPreview).toBeInTheDocument()

    // Verify there's a visual representation (SVG or image)
    const visualElement = dashboardPreview.querySelector('svg') || dashboardPreview.querySelector('img')
    expect(visualElement).toBeInTheDocument()
  })

  // Test Case 2: Key analytics features are listed or highlighted
  it('displays key analytics feature highlights', () => {
    renderWithRouter()

    // Verify analytics features section exists
    const featuresHighlights = screen.getByTestId('analytics-feature-highlights')
    expect(featuresHighlights).toBeInTheDocument()

    // Check for feature highlight items
    const featureItems = screen.getAllByTestId(/^analytics-feature-/)
    expect(featureItems.length).toBeGreaterThanOrEqual(2)

    // Verify features contain analytics-related content
    const sectionText = featuresHighlights.textContent?.toLowerCase() || ''
    expect(sectionText).toMatch(/click|track|analytic|insight|real-time|geo|location|browser|device/i)
  })

  it('renders section heading related to analytics', () => {
    renderWithRouter()

    // Verify section has an appropriate heading
    const heading = screen.getByRole('heading', { level: 2 })
    expect(heading).toBeInTheDocument()
    expect(heading.textContent?.toLowerCase()).toMatch(/analytic|insight|track|dashboard|performance/)
  })

  it('displays a descriptive text about analytics capabilities', () => {
    renderWithRouter()

    // Check for descriptive paragraph
    const description = screen.getByTestId('analytics-description')
    expect(description).toBeInTheDocument()
    expect(description.textContent?.toLowerCase()).toMatch(/track|monitor|analytic|insight|click|performance/)
  })
})

describe('AnalyticsPreviewSection - CTA Integration', () => {
  // Test Case 3: CTA navigates to /register
  it('renders "See Your Analytics" CTA button', () => {
    renderWithRouter()

    // Find CTA button
    const ctaButton = screen.getByTestId('analytics-cta')
    expect(ctaButton).toBeInTheDocument()

    // Verify CTA has appropriate text
    const ctaText = ctaButton.textContent?.toLowerCase() || ''
    expect(ctaText).toMatch(/see.*analytic|view.*analytic|get.*analytic|start.*track|try.*free/)
  })

  it('navigates to /register when clicking analytics CTA', async () => {
    renderWithRouter()

    // Find and click CTA
    const ctaButton = screen.getByTestId('analytics-cta')
    fireEvent.click(ctaButton)

    // Verify navigation to register page
    await waitFor(() => {
      expect(screen.getByTestId('register-page')).toBeInTheDocument()
    })
  })

  it('CTA is a link element pointing to /register', () => {
    renderWithRouter()

    const ctaButton = screen.getByTestId('analytics-cta')

    // Verify it's a link or has proper navigation setup
    const linkElement = ctaButton.closest('a')
    expect(linkElement).toBeInTheDocument()
    expect(linkElement?.getAttribute('href')).toBe('/register')
  })
})

describe('AnalyticsPreviewSection - Visual Elements', () => {
  it('displays chart or graph visualization in preview', () => {
    renderWithRouter()

    const dashboardPreview = screen.getByTestId('analytics-dashboard-preview')

    // Check for chart-related SVG elements (bars, lines, etc.)
    const svgElement = dashboardPreview.querySelector('svg')
    expect(svgElement).toBeInTheDocument()

    // Verify it contains visual chart elements (rect for bars, path/line for lines, etc.)
    const chartElements = svgElement?.querySelectorAll('rect, path, circle, line')
    expect(chartElements?.length).toBeGreaterThan(0)
  })

  it('has proper styling with base theme classes', () => {
    renderWithRouter()

    const section = screen.getByTestId('analytics-preview-section')

    // Verify section has styling classes
    expect(section.className).toMatch(/bg-|py-|px-/)
  })
})
