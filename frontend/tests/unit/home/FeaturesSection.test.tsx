/**
 * Features Section Unit Tests
 * Owner: Scenario 4 - Features Section Display
 *
 * Tests for FeaturesSection component:
 * - Displays exactly 3 feature cards
 * - Each card has icon, title, and description
 * - Features cover: URL shortening, analytics, link management
 * - Uses GlassMorphismCard component
 *
 * Testing framework: Vitest + @testing-library/react
 */
import React from 'react'
import { describe, it, expect, vi } from 'vitest'
import { screen, within } from '@testing-library/react'
import { renderWithProviders } from '../../utils/renderWithProviders'
import { FeaturesSection } from '@/components/home/FeaturesSection'

// Mock GlassMorphismCard to track its usage - using relative path from FeaturesSection
vi.mock('../../../src/components/GlassMorphismCard', () => ({
  GlassMorphismCard: ({ children, className }: { children: React.ReactNode; className?: string }) => (
    React.createElement('div', { 'data-testid': 'glass-morphism-card', className }, children)
  ),
}))

describe('FeaturesSection', () => {
  describe('Test Case 1: Features section exists with exactly 3 feature cards', () => {
    it('should render a features section with exactly 3 feature cards', () => {
      renderWithProviders(<FeaturesSection />)

      // Check features section is present
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Check exactly 3 GlassMorphismCard components are used
      const featureCards = screen.getAllByTestId('glass-morphism-card')
      expect(featureCards).toHaveLength(3)
    })

    it('should render features section as a semantic section element', () => {
      renderWithProviders(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection.tagName.toLowerCase()).toBe('section')
    })
  })

  describe('Test Case 2: URL Shortening feature card content', () => {
    it('should display URL Shortening feature card with title mentioning short/shorten', () => {
      renderWithProviders(<FeaturesSection />)

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
      expect(urlShorteningCard).toBeInTheDocument()

      // Check title contains 'short' or 'shorten'
      const title = within(urlShorteningCard).getByRole('heading', { level: 3 })
      expect(title.textContent?.toLowerCase()).toMatch(/short/)
    })

    it('should have an icon for URL Shortening feature', () => {
      renderWithProviders(<FeaturesSection />)

      const icon = screen.getByTestId('feature-icon-url-shortening')
      expect(icon).toBeInTheDocument()

      // Should contain an SVG icon
      const svg = icon.querySelector('svg')
      expect(svg).toBeInTheDocument()
    })

    it('should have descriptive text for URL Shortening feature', () => {
      renderWithProviders(<FeaturesSection />)

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
      // Get the paragraph element specifically (description)
      const paragraphs = within(urlShorteningCard).getAllByText(/transform|urls|links|short/i)
      // At least one paragraph should contain descriptive text
      expect(paragraphs.some(el => el.tagName.toLowerCase() === 'p')).toBe(true)
    })
  })

  describe('Test Case 3: Analytics feature card content', () => {
    it('should display Analytics feature card with title mentioning analytics/track', () => {
      renderWithProviders(<FeaturesSection />)

      const analyticsCard = screen.getByTestId('feature-card-analytics')
      expect(analyticsCard).toBeInTheDocument()

      // Check title contains 'analytics' or 'track'
      const title = within(analyticsCard).getByRole('heading', { level: 3 })
      expect(title.textContent?.toLowerCase()).toMatch(/analytics|track/)
    })

    it('should have an icon for Analytics feature', () => {
      renderWithProviders(<FeaturesSection />)

      const icon = screen.getByTestId('feature-icon-analytics')
      expect(icon).toBeInTheDocument()

      // Should contain an SVG icon
      const svg = icon.querySelector('svg')
      expect(svg).toBeInTheDocument()
    })

    it('should have descriptive text for Analytics feature', () => {
      renderWithProviders(<FeaturesSection />)

      const analyticsCard = screen.getByTestId('feature-card-analytics')
      // Get elements matching the description text pattern
      const paragraphs = within(analyticsCard).getAllByText(/track|click|analytics|monitor/i)
      // At least one paragraph should contain descriptive text
      expect(paragraphs.some(el => el.tagName.toLowerCase() === 'p')).toBe(true)
    })
  })

  describe('Test Case 4: Link Management feature card content', () => {
    it('should display Link Management feature card with title mentioning manage/organize', () => {
      renderWithProviders(<FeaturesSection />)

      const linkManagementCard = screen.getByTestId('feature-card-link-management')
      expect(linkManagementCard).toBeInTheDocument()

      // Check title contains 'manage' or 'organize'
      const title = within(linkManagementCard).getByRole('heading', { level: 3 })
      expect(title.textContent?.toLowerCase()).toMatch(/manage|organize/)
    })

    it('should have an icon for Link Management feature', () => {
      renderWithProviders(<FeaturesSection />)

      const icon = screen.getByTestId('feature-icon-link-management')
      expect(icon).toBeInTheDocument()

      // Should contain an SVG icon
      const svg = icon.querySelector('svg')
      expect(svg).toBeInTheDocument()
    })

    it('should have descriptive text for Link Management feature', () => {
      renderWithProviders(<FeaturesSection />)

      const linkManagementCard = screen.getByTestId('feature-card-link-management')
      // Get elements matching the description text pattern
      const paragraphs = within(linkManagementCard).getAllByText(/manage|organize|edit|delete/i)
      // At least one paragraph should contain descriptive text
      expect(paragraphs.some(el => el.tagName.toLowerCase() === 'p')).toBe(true)
    })
  })

  describe('Test Case 5: GlassMorphismCard component usage', () => {
    it('should use GlassMorphismCard component for each feature card', () => {
      renderWithProviders(<FeaturesSection />)

      // Each feature should be wrapped in a GlassMorphismCard
      const glassMorphismCards = screen.getAllByTestId('glass-morphism-card')
      expect(glassMorphismCards).toHaveLength(3)

      // Each card should have the feature-card class
      glassMorphismCards.forEach(card => {
        expect(card).toHaveClass('feature-card')
      })
    })
  })

  describe('Features section structure and accessibility', () => {
    it('should have a heading for the features section', () => {
      renderWithProviders(<FeaturesSection />)

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toBeInTheDocument()
      expect(heading.textContent?.toLowerCase()).toMatch(/feature/)
    })

    it('should have proper ARIA labeling', () => {
      renderWithProviders(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading')

      const featuresList = screen.getByRole('list', { name: /features/i })
      expect(featuresList).toBeInTheDocument()
    })

    it('should render all three feature cards with proper structure', () => {
      renderWithProviders(<FeaturesSection />)

      const featureCards = screen.getAllByRole('listitem')
      expect(featureCards).toHaveLength(3)

      featureCards.forEach(card => {
        // Each card should have a heading
        const heading = within(card).getByRole('heading', { level: 3 })
        expect(heading).toBeInTheDocument()
      })
    })
  })
})
