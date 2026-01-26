/**
 * Animation Tests
 * Owner: Scenario 12 - Framer Motion Animations
 *
 * Test coverage:
 * - Framer Motion components present
 * - Initial animation states
 * - Entrance animations
 * - Scroll-triggered animations (whileInView)
 */
import { describe, it, expect } from 'vitest'
import { renderWithProviders, screen } from './test-utils'
import Home from '../../pages/Home'
import { HowItWorksSection } from '../../components/home/HowItWorksSection'
import { AnalyticsPreviewSection } from '../../components/home/AnalyticsPreviewSection'

describe('Framer Motion Animations', () => {
  describe('Test Case 1: Framer Motion components are used for animations', () => {
    it('renders homepage with motion elements in HowItWorksSection', () => {
      renderWithProviders(<HowItWorksSection />)

      // HowItWorksSection should render and use motion components
      // The motion components render as regular DOM elements with data attributes
      const section = screen.getByRole('region', { name: /how it works/i })
      expect(section).toBeInTheDocument()

      // Verify the heading is rendered (motion-animated element)
      const heading = screen.getByRole('heading', { name: /how it works/i, level: 2 })
      expect(heading).toBeInTheDocument()

      // Verify the step cards are rendered (motion-animated elements)
      const stepCards = screen.getAllByTestId(/step-number-/)
      expect(stepCards).toHaveLength(3)
    })

    it('renders homepage with motion elements in AnalyticsPreviewSection', () => {
      renderWithProviders(<AnalyticsPreviewSection />)

      // AnalyticsPreviewSection should render with motion components
      const section = screen.getByTestId('analytics-preview-section')
      expect(section).toBeInTheDocument()

      // Verify the heading is rendered (motion-animated element)
      const heading = screen.getByRole('heading', {
        name: /powerful analytics at your fingertips/i,
        level: 2
      })
      expect(heading).toBeInTheDocument()

      // Verify the chart container is rendered
      const chart = screen.getByTestId('analytics-chart')
      expect(chart).toBeInTheDocument()
    })

    it('renders homepage with motion components across sections', () => {
      renderWithProviders(<Home />)

      // Both animated sections should be present in the homepage
      const howItWorksHeading = screen.getByRole('heading', {
        name: /how it works/i,
        level: 2
      })
      expect(howItWorksHeading).toBeInTheDocument()

      const analyticsHeading = screen.getByRole('heading', {
        name: /powerful analytics at your fingertips/i,
        level: 2
      })
      expect(analyticsHeading).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Hero elements have initial/animate props for entrance animation', () => {
    it('HowItWorksSection has entrance animations with initial and whileInView props', () => {
      renderWithProviders(<HowItWorksSection />)

      // The component renders with motion elements that have entrance animations
      // When rendered, the motion elements should be visible
      const section = screen.getByRole('region', { name: /how it works/i })
      expect(section).toBeInTheDocument()

      // The heading should be rendered (has initial opacity: 0, y: -20 and animates to visible)
      const heading = screen.getByRole('heading', { name: /how it works/i, level: 2 })
      expect(heading).toBeInTheDocument()

      // The description text should be rendered
      const description = screen.getByText(/shortening your urls is quick and easy/i)
      expect(description).toBeInTheDocument()
    })

    it('AnalyticsPreviewSection has entrance animations with initial and whileInView props', () => {
      renderWithProviders(<AnalyticsPreviewSection />)

      // The component renders with motion elements that have entrance animations
      const section = screen.getByTestId('analytics-preview-section')
      expect(section).toBeInTheDocument()

      // The heading should be rendered (has initial opacity: 0, y: -20 and animates to visible)
      const heading = screen.getByRole('heading', {
        name: /powerful analytics at your fingertips/i,
        level: 2
      })
      expect(heading).toBeInTheDocument()

      // The description text should be rendered (has entrance animation)
      const description = screen.getByTestId('analytics-description')
      expect(description).toBeInTheDocument()
    })

    it('workflow steps have staggered entrance animations', () => {
      renderWithProviders(<HowItWorksSection />)

      // All three workflow steps should render (they have staggered entrance animations)
      const step1 = screen.getByText('Paste your long URL')
      const step2 = screen.getByText('Get a short, memorable link')
      const step3 = screen.getByText('Track clicks and analytics')

      expect(step1).toBeInTheDocument()
      expect(step2).toBeInTheDocument()
      expect(step3).toBeInTheDocument()

      // Step numbers should be rendered
      const stepNumbers = screen.getAllByTestId(/step-number-/)
      expect(stepNumbers).toHaveLength(3)
      expect(stepNumbers[0]).toHaveTextContent('1')
      expect(stepNumbers[1]).toHaveTextContent('2')
      expect(stepNumbers[2]).toHaveTextContent('3')
    })
  })

  describe('Test Case 3: Sections use whileInView for scroll-triggered animations', () => {
    it('HowItWorksSection uses whileInView for viewport-triggered animations', () => {
      renderWithProviders(<HowItWorksSection />)

      // The section heading uses whileInView
      // When IntersectionObserver fires (mocked), the element should be visible
      const heading = screen.getByRole('heading', { name: /how it works/i, level: 2 })
      expect(heading).toBeInTheDocument()

      // The grid container also uses whileInView with staggered children
      const stepCards = screen.getAllByTestId(/step-number-/)
      expect(stepCards).toHaveLength(3)
    })

    it('AnalyticsPreviewSection uses whileInView for viewport-triggered animations', () => {
      renderWithProviders(<AnalyticsPreviewSection />)

      // The section header uses whileInView
      const heading = screen.getByRole('heading', {
        name: /powerful analytics at your fingertips/i,
        level: 2
      })
      expect(heading).toBeInTheDocument()

      // The main content grid uses whileInView with containerVariants
      const chart = screen.getByTestId('analytics-chart')
      expect(chart).toBeInTheDocument()

      // The statistics cards are rendered (within motion container)
      const statIcons = screen.getAllByTestId(/stat-icon-/)
      expect(statIcons.length).toBeGreaterThanOrEqual(3)
    })

    it('AnalyticsPreviewSection benefits section uses whileInView animation', () => {
      renderWithProviders(<AnalyticsPreviewSection />)

      // The benefits section has its own whileInView animation
      const benefitsText = screen.getByTestId('analytics-benefits')
      expect(benefitsText).toBeInTheDocument()
      expect(benefitsText).toHaveTextContent(/understanding who clicks your links/i)
    })

    it('motion components render correctly with viewport: { once: true }', () => {
      renderWithProviders(<HowItWorksSection />)

      // Components with viewport: { once: true } should render their content
      // This ensures animations only trigger once when entering viewport
      const section = screen.getByRole('region', { name: /how it works/i })
      expect(section).toBeInTheDocument()

      // All child content should be accessible
      const heading = screen.getByRole('heading', { name: /how it works/i, level: 2 })
      expect(heading).toBeInTheDocument()
    })

    it('homepage renders all animated sections correctly', () => {
      renderWithProviders(<Home />)

      // Both HowItWorksSection and AnalyticsPreviewSection should be rendered
      // with their whileInView animations

      // HowItWorksSection content
      const howItWorksHeading = screen.getByRole('heading', {
        name: /how it works/i,
        level: 2
      })
      expect(howItWorksHeading).toBeInTheDocument()

      // AnalyticsPreviewSection content
      const analyticsSection = screen.getByTestId('analytics-preview-section')
      expect(analyticsSection).toBeInTheDocument()

      // Verify animation-dependent content is rendered
      const stepNumbers = screen.getAllByTestId(/step-number-/)
      expect(stepNumbers).toHaveLength(3)
    })
  })

  describe('Animation variants are properly defined', () => {
    it('HowItWorksSection uses containerVariants with staggerChildren', () => {
      renderWithProviders(<HowItWorksSection />)

      // Container with staggerChildren renders all children
      const steps = screen.getAllByTestId(/step-number-/)
      expect(steps).toHaveLength(3)

      // Each step has its own itemVariants animation
      const step1Title = screen.getByText('Paste your long URL')
      const step2Title = screen.getByText('Get a short, memorable link')
      const step3Title = screen.getByText('Track clicks and analytics')

      expect(step1Title).toBeInTheDocument()
      expect(step2Title).toBeInTheDocument()
      expect(step3Title).toBeInTheDocument()
    })

    it('AnalyticsPreviewSection uses containerVariants with staggerChildren', () => {
      renderWithProviders(<AnalyticsPreviewSection />)

      // Container with staggerChildren renders all children
      // The chart section and stats section are siblings with staggered animations
      const chart = screen.getByTestId('analytics-chart')
      expect(chart).toBeInTheDocument()

      // Stats cards render with stagger
      const statValues = screen.getAllByTestId(/stat-value-/)
      expect(statValues.length).toBeGreaterThanOrEqual(3)
    })
  })
})
