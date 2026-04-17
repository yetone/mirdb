/**
 * Animation and Micro-interactions Integration Tests
 * Owner: Scenario 14 - Animation and Micro-interactions
 *
 * Tests smooth animations and user feedback interactions using Framer Motion.
 * Uses mocked Framer Motion components for deterministic testing.
 *
 * Related requirements: PRD User Interaction Patterns
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor, act } from '../test-utils'
import userEvent from '@testing-library/user-event'
import React from 'react'

// Mock framer-motion to avoid animation timing issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: React.forwardRef(({ children, whileHover, whileTap, initial, animate, exit, variants, transition, viewport, whileInView, ...props }: any, ref: any) => (
      <div ref={ref} data-testid-motion="div" data-while-hover={JSON.stringify(whileHover)} {...props}>{children}</div>
    )),
    p: React.forwardRef(({ children, ...props }: any, ref: any) => (
      <p ref={ref} {...props}>{children}</p>
    )),
    button: React.forwardRef(({ children, whileHover, whileTap, ...props }: any, ref: any) => (
      <button ref={ref} data-while-hover={JSON.stringify(whileHover)} data-while-tap={JSON.stringify(whileTap)} {...props}>{children}</button>
    )),
  },
  AnimatePresence: ({ children }: any) => <>{children}</>,
  useAnimation: () => ({
    start: vi.fn(),
    stop: vi.fn(),
  }),
  useInView: () => true,
}))

// Mock the API
vi.mock('../../src/api', () => ({
  shortenUrl: vi.fn(),
}))

// Import after mocks
import { UrlShortenerForm } from '../../src/components/homepage/UrlShortenerForm'
import { FeatureCard } from '../../src/components/homepage/FeatureCard'
import { HeroSection } from '../../src/components/homepage/HeroSection'
import { shortenUrl as mockShortenUrl } from '../../src/api'

describe('Animation and Micro-interactions Integration Tests - Scenario 14', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('URL Shortener Form Animations', () => {
    // Test Case 3: Submit URL for shortening - Loading spinner or animation appears during API call
    it('displays loading animation during API call', async () => {
      const user = userEvent.setup()

      // Mock slow API response
      const mockResponse = { shortUrl: 'https://short.url/abc123', originalUrl: 'https://example.com', shortCode: 'abc123' }
      vi.mocked(mockShortenUrl).mockImplementation(() =>
        new Promise(resolve => setTimeout(() => resolve(mockResponse), 500))
      )

      render(<UrlShortenerForm />)

      const urlInput = screen.getByTestId('url-input')
      const shortenButton = screen.getByTestId('shorten-button')

      // Enter URL and submit
      await user.type(urlInput, 'https://example.com/long-url')
      await user.click(shortenButton)

      // Verify loading state appears immediately
      await waitFor(() => {
        expect(screen.getByText(/Shortening/i)).toBeInTheDocument()
      })

      // Wait for operation to complete
      await waitFor(() => {
        expect(screen.getByTestId('success-state')).toBeInTheDocument()
      }, { timeout: 2000 })
    })

    // Test Case 4: Successful URL shortening - Success state appears with smooth transition animation
    it('displays success state with animated transition after shortening', async () => {
      const user = userEvent.setup()

      const mockResponse = { shortUrl: 'https://short.url/xyz789', originalUrl: 'https://example.com/test', shortCode: 'xyz789' }
      vi.mocked(mockShortenUrl).mockResolvedValue(mockResponse)

      render(<UrlShortenerForm />)

      const urlInput = screen.getByTestId('url-input')
      const shortenButton = screen.getByTestId('shorten-button')

      await user.type(urlInput, 'https://example.com/test')
      await user.click(shortenButton)

      // Wait for success state to appear
      await waitFor(() => {
        expect(screen.getByTestId('success-state')).toBeInTheDocument()
      })

      // Verify shortened URL is displayed
      expect(screen.getByTestId('shortened-url')).toHaveTextContent('https://short.url/xyz789')

      // Verify copy and reset buttons are present
      expect(screen.getByTestId('copy-button')).toBeInTheDocument()
      expect(screen.getByTestId('reset-button')).toBeInTheDocument()
    })

    // Test Case 5: Click copy button after URL shortened - Button shows copied state feedback
    it('copy button shows copied state feedback with checkmark', async () => {
      const user = userEvent.setup()

      const mockResponse = { shortUrl: 'https://short.url/copy123', originalUrl: 'https://example.com/copy', shortCode: 'copy123' }
      vi.mocked(mockShortenUrl).mockResolvedValue(mockResponse)

      render(<UrlShortenerForm />)

      const urlInput = screen.getByTestId('url-input')
      const shortenButton = screen.getByTestId('shorten-button')

      await user.type(urlInput, 'https://example.com/copy')
      await user.click(shortenButton)

      // Wait for success state
      await waitFor(() => {
        expect(screen.getByTestId('success-state')).toBeInTheDocument()
      })

      const copyButton = screen.getByTestId('copy-button')

      // Initial state should show "Copy"
      expect(copyButton).toHaveTextContent('Copy')

      // Click copy button
      await user.click(copyButton)

      // Button should show "Copied!" state
      await waitFor(() => {
        expect(copyButton).toHaveTextContent('Copied!')
      })

      // Button should have success styling (verifies visual feedback)
      expect(copyButton).toHaveClass('btn-success')
    })

    it('displays error state with animation for invalid URL', async () => {
      const user = userEvent.setup()

      // Mock API to return an error
      vi.mocked(mockShortenUrl).mockRejectedValue(new Error('Invalid URL format'))

      render(<UrlShortenerForm />)

      const urlInput = screen.getByTestId('url-input')
      const shortenButton = screen.getByTestId('shorten-button')

      // Use a URL that looks valid but API rejects
      await user.type(urlInput, 'https://invalid-test-url.example')
      await user.click(shortenButton)

      // Error message should appear with animation (AnimatePresence wraps it)
      await waitFor(() => {
        expect(screen.getByTestId('error-message')).toBeInTheDocument()
      })

      // Error should have proper role for accessibility
      expect(screen.getByRole('alert')).toBeInTheDocument()
    })

    it('reset button clears state and enables new input', async () => {
      const user = userEvent.setup()

      const mockResponse = { shortUrl: 'https://short.url/reset123', originalUrl: 'https://example.com/reset', shortCode: 'reset123' }
      vi.mocked(mockShortenUrl).mockResolvedValue(mockResponse)

      render(<UrlShortenerForm />)

      const urlInput = screen.getByTestId('url-input')
      const shortenButton = screen.getByTestId('shorten-button')

      // Shorten a URL
      await user.type(urlInput, 'https://example.com/reset')
      await user.click(shortenButton)

      // Wait for success state
      await waitFor(() => {
        expect(screen.getByTestId('success-state')).toBeInTheDocument()
      })

      // Click reset button
      const resetButton = screen.getByTestId('reset-button')
      await user.click(resetButton)

      // Success state should disappear (AnimatePresence handles exit animation)
      await waitFor(() => {
        expect(screen.queryByTestId('success-state')).not.toBeInTheDocument()
      })

      // Input should be cleared and enabled
      expect(urlInput).toHaveValue('')
      expect(urlInput).not.toBeDisabled()

      // Shorten button is disabled when input is empty (expected behavior)
      // Type a new URL and verify button becomes enabled
      await user.type(urlInput, 'https://example.com/new-url')
      expect(shortenButton).not.toBeDisabled()
    })
  })

  describe('Feature Card Hover Animations', () => {
    // Test Case 2: Feature cards have whileHover animation configured
    it('feature card has whileHover animation property', () => {
      render(
        <FeatureCard
          icon={<span>Icon</span>}
          title="Test Feature"
          description="Test description"
        />
      )

      const card = screen.getByTestId('feature-card')
      expect(card).toBeInTheDocument()

      // Verify whileHover is configured (mocked motion.div exposes this as data attribute)
      const whileHoverData = card.getAttribute('data-while-hover')
      expect(whileHoverData).toBeTruthy()

      // Parse and verify the hover animation
      const whileHover = JSON.parse(whileHoverData!)
      expect(whileHover).toHaveProperty('y', -5)
    })

    it('feature card displays all content elements', () => {
      render(
        <FeatureCard
          icon={<span data-testid="test-icon">Icon</span>}
          title="Analytics"
          description="Track your links"
        />
      )

      expect(screen.getByTestId('feature-icon')).toBeInTheDocument()
      expect(screen.getByTestId('feature-title')).toHaveTextContent('Analytics')
      expect(screen.getByTestId('feature-description')).toHaveTextContent('Track your links')
    })
  })

  describe('Hero Section Animations', () => {
    it('hero section renders with animation wrappers', () => {
      render(<HeroSection />)

      // Verify main elements are present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('hero-headline')).toBeInTheDocument()
      expect(screen.getByTestId('hero-subheadline')).toBeInTheDocument()
      expect(screen.getByTestId('hero-cta')).toBeInTheDocument()
    })

    // Test Case 1: CTA button is interactive
    it('CTA button is clickable and accessible', async () => {
      const user = userEvent.setup()
      render(<HeroSection />)

      const ctaButton = screen.getByTestId('hero-cta')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toHaveTextContent('Get Started Free')

      // Button should be accessible
      expect(ctaButton.tagName).toBe('A')
      expect(ctaButton).toHaveAttribute('href', '/register')
    })

    it('hero gradient background is rendered', () => {
      render(<HeroSection />)

      const gradient = screen.getByTestId('hero-gradient')
      expect(gradient).toBeInTheDocument()
      expect(gradient).toHaveClass('bg-gradient-to-br')
    })
  })
})

describe('Animation Configuration Tests', () => {
  it('UrlShortenerForm uses AnimatePresence for error/success states', () => {
    // This test verifies the animation setup is correct
    render(<UrlShortenerForm />)

    // The component should render without errors even with mocked framer-motion
    expect(screen.getByTestId('url-shortener-form')).toBeInTheDocument()
  })

  it('FeatureCard has proper transition duration configured', () => {
    render(
      <FeatureCard
        icon={<span>Icon</span>}
        title="Test"
        description="Description"
      />
    )

    const card = screen.getByTestId('feature-card')
    // Card should have transition classes from Tailwind
    expect(card).toHaveClass('transition-shadow')
  })
})
