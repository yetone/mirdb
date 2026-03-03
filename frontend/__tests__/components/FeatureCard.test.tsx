/**
 * FeatureCard Component Tests
 * Owner: Scenario 4 - Feature Highlights Display
 *
 * Tests for the FeatureCard component and FeaturesSection
 */
import React from 'react'
import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { FeatureCard, FeaturesSection, FEATURES } from '../../src/components/FeatureCard'

describe('FeatureCard', () => {
  describe('Test Case 2: Individual feature card rendering', () => {
    it('displays icon, feature name, and description', () => {
      const testFeature = {
        icon: '⚡',
        title: 'Test Feature',
        description: 'This is a test description for the feature.',
      }

      render(
        <FeatureCard
          icon={testFeature.icon}
          title={testFeature.title}
          description={testFeature.description}
        />
      )

      // Check icon is displayed
      const iconElement = screen.getByTestId('feature-icon')
      expect(iconElement).toBeInTheDocument()
      expect(iconElement).toHaveTextContent(testFeature.icon)

      // Check title is displayed
      const titleElement = screen.getByTestId('feature-title')
      expect(titleElement).toBeInTheDocument()
      expect(titleElement).toHaveTextContent(testFeature.title)

      // Check description is displayed
      const descriptionElement = screen.getByTestId('feature-description')
      expect(descriptionElement).toBeInTheDocument()
      expect(descriptionElement).toHaveTextContent(testFeature.description)
    })

    it('renders icon with proper aria-label for accessibility', () => {
      const testFeature = {
        icon: '📊',
        title: 'Analytics',
        description: 'Track your analytics.',
      }

      render(
        <FeatureCard
          icon={testFeature.icon}
          title={testFeature.title}
          description={testFeature.description}
        />
      )

      const iconElement = screen.getByRole('img', { name: testFeature.title })
      expect(iconElement).toBeInTheDocument()
    })
  })

  describe('Test Case 3: GlassMorphismCard usage', () => {
    it('renders using GlassMorphismCard component with characteristic styles', () => {
      const testFeature = {
        icon: '🎨',
        title: 'Themes',
        description: 'Customize your experience.',
      }

      render(
        <FeatureCard
          icon={testFeature.icon}
          title={testFeature.title}
          description={testFeature.description}
        />
      )

      const cardWrapper = screen.getByTestId('feature-card')
      expect(cardWrapper).toBeInTheDocument()

      // GlassMorphismCard applies these classes to its container
      const glassMorphismCard = cardWrapper.querySelector('.card')
      expect(glassMorphismCard).toBeInTheDocument()
      expect(glassMorphismCard).toHaveClass('bg-base-100/80')
      expect(glassMorphismCard).toHaveClass('backdrop-blur-md')
      expect(glassMorphismCard).toHaveClass('border')
      expect(glassMorphismCard).toHaveClass('border-base-300')
      expect(glassMorphismCard).toHaveClass('shadow-xl')
    })

    it('has hover effect classes for desktop interaction', () => {
      const testFeature = {
        icon: '⚡',
        title: 'Fast',
        description: 'Lightning fast.',
      }

      render(
        <FeatureCard
          icon={testFeature.icon}
          title={testFeature.title}
          description={testFeature.description}
        />
      )

      const cardWrapper = screen.getByTestId('feature-card')
      const glassMorphismCard = cardWrapper.querySelector('.card')

      // Check for hover transition classes
      expect(glassMorphismCard).toHaveClass('transition-all')
      expect(glassMorphismCard).toHaveClass('duration-300')
      expect(glassMorphismCard).toHaveClass('hover:scale-105')
      expect(glassMorphismCard).toHaveClass('hover:shadow-2xl')
      expect(glassMorphismCard).toHaveClass('hover:border-primary')
    })
  })
})

describe('FeaturesSection', () => {
  describe('Test Case 1: Features section renders 3-5 feature cards', () => {
    it('renders between 3 and 5 feature cards', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThanOrEqual(3)
      expect(featureCards.length).toBeLessThanOrEqual(5)
    })

    it('renders exactly the number of features defined in FEATURES array', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBe(FEATURES.length)
    })

    it('renders all features from FEATURES array with correct content', () => {
      render(<FeaturesSection />)

      FEATURES.forEach((feature) => {
        expect(screen.getByText(feature.title)).toBeInTheDocument()
        expect(screen.getByText(feature.description)).toBeInTheDocument()
      })
    })
  })

  it('renders features section with proper heading', () => {
    render(<FeaturesSection />)

    const heading = screen.getByRole('heading', { name: /why choose us/i })
    expect(heading).toBeInTheDocument()
  })

  it('renders features section with aria-labelledby for accessibility', () => {
    render(<FeaturesSection />)

    const section = screen.getByTestId('features-section')
    expect(section).toHaveAttribute('aria-labelledby', 'features-heading')

    const heading = document.getElementById('features-heading')
    expect(heading).toBeInTheDocument()
  })

  it('renders features in a responsive grid container', () => {
    render(<FeaturesSection />)

    // Find the grid container
    const featureCards = screen.getAllByTestId('feature-card')
    const gridContainer = featureCards[0].parentElement

    expect(gridContainer).toHaveClass('grid')
    expect(gridContainer).toHaveClass('grid-cols-1')
    expect(gridContainer).toHaveClass('md:grid-cols-2')
    expect(gridContainer).toHaveClass('lg:grid-cols-3')
    expect(gridContainer).toHaveClass('gap-6')
  })
})

describe('FEATURES constant', () => {
  it('contains between 3 and 5 features', () => {
    expect(FEATURES.length).toBeGreaterThanOrEqual(3)
    expect(FEATURES.length).toBeLessThanOrEqual(5)
  })

  it('each feature has required properties: icon, title, description', () => {
    FEATURES.forEach((feature, index) => {
      expect(feature).toHaveProperty('icon')
      expect(feature).toHaveProperty('title')
      expect(feature).toHaveProperty('description')

      expect(typeof feature.icon).toBe('string')
      expect(typeof feature.title).toBe('string')
      expect(typeof feature.description).toBe('string')

      expect(feature.icon.length).toBeGreaterThan(0)
      expect(feature.title.length).toBeGreaterThan(0)
      expect(feature.description.length).toBeGreaterThan(0)
    })
  })
})
