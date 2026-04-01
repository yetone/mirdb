/**
 * FeatureCard Component Isolation Tests.
 * Owner: Scenario 17 - Component Isolation and Props
 *
 * Tests FeatureCard component in isolation with various prop combinations
 * to verify proper rendering and prop handling.
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { FeatureCard } from './FeatureCard'

describe('FeatureCard Component Isolation', () => {
  describe('Test Case 2: Props rendering', () => {
    it('renders all provided props correctly - icon, title, description', () => {
      render(
        <FeatureCard
          icon="🚀"
          title="Test Feature"
          description="This is a test description for the feature card"
          status="complete"
        />
      )

      // Verify icon is rendered
      const icon = screen.getByTestId('feature-icon')
      expect(icon).toBeInTheDocument()
      expect(icon).toHaveTextContent('🚀')

      // Verify title is rendered
      const title = screen.getByTestId('feature-title')
      expect(title).toBeInTheDocument()
      expect(title).toHaveTextContent('Test Feature')

      // Verify description is rendered
      const description = screen.getByTestId('feature-description')
      expect(description).toBeInTheDocument()
      expect(description).toHaveTextContent('This is a test description for the feature card')
    })

    it('renders with different icon values', () => {
      const { rerender } = render(
        <FeatureCard icon="⚡" title="Speed" description="Fast" status="complete" />
      )

      expect(screen.getByTestId('feature-icon')).toHaveTextContent('⚡')

      rerender(
        <FeatureCard icon="🔧" title="Tools" description="Useful" status="complete" />
      )

      expect(screen.getByTestId('feature-icon')).toHaveTextContent('🔧')
    })

    it('renders with different title values', () => {
      const { rerender } = render(
        <FeatureCard icon="📦" title="First Title" description="Desc" status="complete" />
      )

      expect(screen.getByTestId('feature-title')).toHaveTextContent('First Title')

      rerender(
        <FeatureCard icon="📦" title="Second Title" description="Desc" status="complete" />
      )

      expect(screen.getByTestId('feature-title')).toHaveTextContent('Second Title')
    })

    it('renders with different description values', () => {
      const { rerender } = render(
        <FeatureCard icon="📦" title="Title" description="First description" status="complete" />
      )

      expect(screen.getByTestId('feature-description')).toHaveTextContent('First description')

      rerender(
        <FeatureCard icon="📦" title="Title" description="Second description" status="complete" />
      )

      expect(screen.getByTestId('feature-description')).toHaveTextContent('Second description')
    })
  })

  describe('Test Case 3: Status indicators', () => {
    it('shows checkmark indicator for completed status', () => {
      render(
        <FeatureCard
          icon="✅"
          title="Completed Feature"
          description="This feature is done"
          status="complete"
        />
      )

      const indicator = screen.getByTestId('status-complete')
      expect(indicator).toBeInTheDocument()
      expect(indicator).toHaveTextContent('✓')
      expect(indicator).toHaveClass('text-green-500')
    })

    it('shows circle indicator for in-progress status', () => {
      render(
        <FeatureCard
          icon="🔄"
          title="In Progress Feature"
          description="Working on this"
          status="in-progress"
        />
      )

      const indicator = screen.getByTestId('status-in-progress')
      expect(indicator).toBeInTheDocument()
      expect(indicator).toHaveTextContent('○')
      expect(indicator).toHaveClass('text-yellow-500')
    })

    it('shows diamond indicator for planned status', () => {
      render(
        <FeatureCard
          icon="📋"
          title="Planned Feature"
          description="Coming soon"
          status="planned"
        />
      )

      const indicator = screen.getByTestId('status-planned')
      expect(indicator).toBeInTheDocument()
      expect(indicator).toHaveTextContent('◇')
      expect(indicator).toHaveClass('text-gray-400')
    })
  })

  describe('Component structure', () => {
    it('renders as a div element with card styling', () => {
      render(
        <FeatureCard icon="🎯" title="Card" description="Test" status="complete" />
      )

      const card = screen.getByTestId('feature-card')
      expect(card).toBeInTheDocument()
      expect(card.tagName).toBe('DIV')
      expect(card).toHaveClass('p-6', 'rounded-lg', 'shadow-md')
    })

    it('renders with proper dark mode classes', () => {
      render(
        <FeatureCard icon="🌙" title="Dark Mode" description="Support" status="complete" />
      )

      const card = screen.getByTestId('feature-card')
      expect(card.className).toContain('dark:bg-gray-800')
    })

    it('renders icon with aria-hidden for accessibility', () => {
      render(
        <FeatureCard icon="🎨" title="Icon Test" description="Accessibility" status="complete" />
      )

      const icon = screen.getByTestId('feature-icon')
      expect(icon).toHaveAttribute('aria-hidden', 'true')
    })

    it('renders status indicator with aria-label', () => {
      render(
        <FeatureCard icon="🏷️" title="Label Test" description="Testing" status="complete" />
      )

      const indicator = screen.getByTestId('status-complete')
      expect(indicator).toHaveAttribute('aria-label', 'Completed')
    })
  })

  describe('Prop type validation', () => {
    it('handles empty string props gracefully', () => {
      render(
        <FeatureCard icon="" title="" description="" status="complete" />
      )

      const card = screen.getByTestId('feature-card')
      expect(card).toBeInTheDocument()
    })

    it('handles long text props gracefully', () => {
      const longTitle = 'A'.repeat(100)
      const longDescription = 'B'.repeat(500)

      render(
        <FeatureCard icon="📝" title={longTitle} description={longDescription} status="complete" />
      )

      const title = screen.getByTestId('feature-title')
      const description = screen.getByTestId('feature-description')

      expect(title).toHaveTextContent(longTitle)
      expect(description).toHaveTextContent(longDescription)
    })

    it('handles special characters in props', () => {
      render(
        <FeatureCard
          icon="🔐"
          title="Test <script>alert('xss')</script>"
          description="Content with & special < characters >"
          status="complete"
        />
      )

      const title = screen.getByTestId('feature-title')
      const description = screen.getByTestId('feature-description')

      expect(title).toHaveTextContent("<script>alert('xss')</script>")
      expect(description).toHaveTextContent('Content with & special < characters >')
    })
  })
})
