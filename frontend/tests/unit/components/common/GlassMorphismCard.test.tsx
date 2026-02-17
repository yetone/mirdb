/**
 * GlassMorphismCard component tests.
 * Scenario 13 - Component Integration
 *
 * Tests:
 * - Component renders with glassmorphism styling
 * - Feature cards in FeaturesSection use GlassMorphismCard
 * - Visual consistency with theme-aware classes
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { GlassMorphismCard } from '../../../../src/components/common/GlassMorphismCard'
import { FeaturesSection } from '../../../../src/components/home/FeaturesSection'
import { ThemeProvider } from '../../../../src/contexts/ThemeContext'

// Helper to wrap components with necessary providers
function renderWithProviders(ui: React.ReactElement) {
  return render(
    <BrowserRouter>
      <ThemeProvider>{ui}</ThemeProvider>
    </BrowserRouter>
  )
}

describe('GlassMorphismCard', () => {
  it('renders children correctly', () => {
    render(
      <GlassMorphismCard testId="test-card">
        <p>Test Content</p>
      </GlassMorphismCard>
    )

    expect(screen.getByText('Test Content')).toBeInTheDocument()
    expect(screen.getByTestId('test-card')).toBeInTheDocument()
  })

  it('applies glassmorphism styling classes', () => {
    render(
      <GlassMorphismCard testId="glass-card">
        <span>Content</span>
      </GlassMorphismCard>
    )

    const card = screen.getByTestId('glass-card')
    expect(card).toHaveClass('backdrop-blur-md')
    expect(card).toHaveClass('bg-base-100/70')
    expect(card).toHaveClass('rounded-2xl')
    expect(card).toHaveClass('shadow-xl')
  })

  it('accepts custom className', () => {
    render(
      <GlassMorphismCard testId="custom-card" className="custom-class">
        <span>Content</span>
      </GlassMorphismCard>
    )

    const card = screen.getByTestId('custom-card')
    expect(card).toHaveClass('custom-class')
  })
})

describe('GlassMorphismCard Integration - FeaturesSection', () => {
  it('feature cards use GlassMorphismCard component for visual consistency', () => {
    renderWithProviders(<FeaturesSection />)

    // FeaturesSection should render 4 feature cards using GlassMorphismCard
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    const analyticsCard = screen.getByTestId('feature-card-analytics')
    const secureManagementCard = screen.getByTestId('feature-card-secure-management')
    const customCodesCard = screen.getByTestId('feature-card-custom-codes')

    // Verify all cards exist
    expect(urlShorteningCard).toBeInTheDocument()
    expect(analyticsCard).toBeInTheDocument()
    expect(secureManagementCard).toBeInTheDocument()
    expect(customCodesCard).toBeInTheDocument()

    // Verify all cards have glassmorphism styling
    const cards = [urlShorteningCard, analyticsCard, secureManagementCard, customCodesCard]
    cards.forEach((card) => {
      expect(card).toHaveClass('backdrop-blur-md')
      expect(card).toHaveClass('bg-base-100/70')
      expect(card).toHaveClass('rounded-2xl')
    })
  })

  it('feature cards contain titles and descriptions', () => {
    renderWithProviders(<FeaturesSection />)

    // Verify feature titles are present
    expect(screen.getByText('URL Shortening')).toBeInTheDocument()
    expect(screen.getByText('Click Analytics')).toBeInTheDocument()
    expect(screen.getByText('Secure Management')).toBeInTheDocument()
    expect(screen.getByText('Custom Short Codes')).toBeInTheDocument()
  })
})
