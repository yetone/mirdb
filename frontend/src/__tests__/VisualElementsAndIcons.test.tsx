import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import FeaturesSection from '../components/FeaturesSection'
import HowItWorksSection from '../components/HowItWorksSection'

/**
 * Visual Elements and Icons Test Suite
 *
 * Verifies REQ-6: Visual elements (icons, illustrations) reinforce the product's modern brand
 *
 * Test Cases:
 * 1. URL Shortening feature displays relevant link icon
 * 2. Analytics feature displays relevant chart icon
 * 3. Link Management feature displays relevant folder/list icon
 * 4. How It Works section has visual elements for each step
 */
describe('Visual Elements and Icons (REQ-6)', () => {
  describe('Test Case 1: URL Shortening Feature Icon', () => {
    it('should display a relevant link icon for URL Shortening feature', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('url-shortening-icon')
      expect(icon).toBeInTheDocument()

      // Verify it's an SVG element
      expect(icon.tagName.toLowerCase()).toBe('svg')
    })

    it('should have proper SVG structure for URL Shortening icon', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('url-shortening-icon')

      // Check SVG attributes for modern, consistent styling
      expect(icon).toHaveAttribute('viewBox', '0 0 24 24')
      expect(icon).toHaveAttribute('fill', 'none')
      expect(icon).toHaveAttribute('stroke', 'currentColor')
      expect(icon).toHaveAttribute('stroke-width', '1.5')
    })

    it('should have consistent size styling for URL Shortening icon', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('url-shortening-icon')

      // Check for consistent sizing classes
      expect(icon).toHaveClass('w-12')
      expect(icon).toHaveClass('h-12')
    })

    it('should contain link-related path representing URL/chain connection', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('url-shortening-icon')
      const path = icon.querySelector('path')

      expect(path).toBeInTheDocument()
      expect(path).toHaveAttribute('stroke-linecap', 'round')
      expect(path).toHaveAttribute('stroke-linejoin', 'round')
    })
  })

  describe('Test Case 2: Analytics Feature Icon', () => {
    it('should display a relevant chart icon for Analytics feature', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('analytics-icon')
      expect(icon).toBeInTheDocument()

      // Verify it's an SVG element
      expect(icon.tagName.toLowerCase()).toBe('svg')
    })

    it('should have proper SVG structure for Analytics icon', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('analytics-icon')

      // Check SVG attributes for modern, consistent styling
      expect(icon).toHaveAttribute('viewBox', '0 0 24 24')
      expect(icon).toHaveAttribute('fill', 'none')
      expect(icon).toHaveAttribute('stroke', 'currentColor')
      expect(icon).toHaveAttribute('stroke-width', '1.5')
    })

    it('should have consistent size styling for Analytics icon', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('analytics-icon')

      // Check for consistent sizing classes
      expect(icon).toHaveClass('w-12')
      expect(icon).toHaveClass('h-12')
    })

    it('should contain chart-related path representing analytics visualization', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('analytics-icon')
      const path = icon.querySelector('path')

      expect(path).toBeInTheDocument()
      expect(path).toHaveAttribute('stroke-linecap', 'round')
      expect(path).toHaveAttribute('stroke-linejoin', 'round')
    })
  })

  describe('Test Case 3: Link Management Feature Icon', () => {
    it('should display a relevant management/folder icon for Link Management feature', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('link-management-icon')
      expect(icon).toBeInTheDocument()

      // Verify it's an SVG element
      expect(icon.tagName.toLowerCase()).toBe('svg')
    })

    it('should have proper SVG structure for Link Management icon', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('link-management-icon')

      // Check SVG attributes for modern, consistent styling
      expect(icon).toHaveAttribute('viewBox', '0 0 24 24')
      expect(icon).toHaveAttribute('fill', 'none')
      expect(icon).toHaveAttribute('stroke', 'currentColor')
      expect(icon).toHaveAttribute('stroke-width', '1.5')
    })

    it('should have consistent size styling for Link Management icon', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('link-management-icon')

      // Check for consistent sizing classes
      expect(icon).toHaveClass('w-12')
      expect(icon).toHaveClass('h-12')
    })

    it('should contain management-related path representing organization', () => {
      render(<FeaturesSection />)

      const icon = screen.getByTestId('link-management-icon')
      const path = icon.querySelector('path')

      expect(path).toBeInTheDocument()
      expect(path).toHaveAttribute('stroke-linecap', 'round')
      expect(path).toHaveAttribute('stroke-linejoin', 'round')
    })
  })

  describe('Test Case 4: How It Works Step Visuals', () => {
    it('should display visual icon for Step 1 (Paste URL)', () => {
      render(<HowItWorksSection />)

      const step1Card = screen.getByTestId('step-card-1')
      const icon = within(step1Card).getByTestId('step-icon-1')

      expect(icon).toBeInTheDocument()
    })

    it('should display visual icon for Step 2 (Get Shortened Link)', () => {
      render(<HowItWorksSection />)

      const step2Card = screen.getByTestId('step-card-2')
      const icon = within(step2Card).getByTestId('step-icon-2')

      expect(icon).toBeInTheDocument()
    })

    it('should display visual icon for Step 3 (Share and Track)', () => {
      render(<HowItWorksSection />)

      const step3Card = screen.getByTestId('step-card-3')
      const icon = within(step3Card).getByTestId('step-icon-3')

      expect(icon).toBeInTheDocument()
    })

    it('should have SVG icons in all How It Works steps', () => {
      render(<HowItWorksSection />)

      const stepIcons = [
        screen.getByTestId('step-icon-1'),
        screen.getByTestId('step-icon-2'),
        screen.getByTestId('step-icon-3'),
      ]

      stepIcons.forEach((iconContainer) => {
        const svg = iconContainer.querySelector('svg')
        expect(svg).toBeInTheDocument()
      })
    })

    it('should have consistent icon styling across all How It Works steps', () => {
      render(<HowItWorksSection />)

      const stepIcons = [
        screen.getByTestId('step-icon-1'),
        screen.getByTestId('step-icon-2'),
        screen.getByTestId('step-icon-3'),
      ]

      stepIcons.forEach((iconContainer) => {
        const svg = iconContainer.querySelector('svg')
        expect(svg).toHaveAttribute('viewBox', '0 0 24 24')
        expect(svg).toHaveAttribute('fill', 'none')
        expect(svg).toHaveAttribute('stroke', 'currentColor')
        expect(svg).toHaveAttribute('stroke-width', '1.5')
      })
    })

    it('should have step number badges for visual hierarchy', () => {
      render(<HowItWorksSection />)

      const stepNumbers = screen.getAllByTestId(/^step-number-/)
      expect(stepNumbers).toHaveLength(3)

      stepNumbers.forEach((stepNumber) => {
        // Step numbers should be styled as badges
        expect(stepNumber).toHaveClass('flex')
        expect(stepNumber).toHaveClass('items-center')
        expect(stepNumber).toHaveClass('justify-center')
        expect(stepNumber).toHaveClass('rounded-full')
        expect(stepNumber).toHaveClass('bg-primary')
      })
    })
  })

  describe('Visual Consistency Across All Icons', () => {
    it('should use consistent stroke width across all feature icons', () => {
      render(<FeaturesSection />)

      const icons = [
        screen.getByTestId('url-shortening-icon'),
        screen.getByTestId('analytics-icon'),
        screen.getByTestId('link-management-icon'),
      ]

      icons.forEach((icon) => {
        expect(icon).toHaveAttribute('stroke-width', '1.5')
      })
    })

    it('should use consistent viewBox across all feature icons', () => {
      render(<FeaturesSection />)

      const icons = [
        screen.getByTestId('url-shortening-icon'),
        screen.getByTestId('analytics-icon'),
        screen.getByTestId('link-management-icon'),
      ]

      icons.forEach((icon) => {
        expect(icon).toHaveAttribute('viewBox', '0 0 24 24')
      })
    })

    it('should use outline style (fill: none, stroke: currentColor) for modern appearance', () => {
      render(<FeaturesSection />)

      const icons = [
        screen.getByTestId('url-shortening-icon'),
        screen.getByTestId('analytics-icon'),
        screen.getByTestId('link-management-icon'),
      ]

      icons.forEach((icon) => {
        expect(icon).toHaveAttribute('fill', 'none')
        expect(icon).toHaveAttribute('stroke', 'currentColor')
      })
    })

    it('should use consistent sizing (w-12 h-12) for all feature icons', () => {
      render(<FeaturesSection />)

      const icons = [
        screen.getByTestId('url-shortening-icon'),
        screen.getByTestId('analytics-icon'),
        screen.getByTestId('link-management-icon'),
      ]

      icons.forEach((icon) => {
        expect(icon).toHaveClass('w-12')
        expect(icon).toHaveClass('h-12')
      })
    })

    it('should have icons wrapped in primary color containers', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId(/^feature-card-/)

      featureCards.forEach((card) => {
        const iconContainer = card.querySelector('.text-primary')
        expect(iconContainer).toBeInTheDocument()
      })
    })

    it('should maintain visual consistency between Features and How It Works sections', () => {
      const { unmount: unmountFeatures } = render(<FeaturesSection />)

      const featureIcon = screen.getByTestId('url-shortening-icon')
      const featureIconAttrs = {
        viewBox: featureIcon.getAttribute('viewBox'),
        fill: featureIcon.getAttribute('fill'),
        stroke: featureIcon.getAttribute('stroke'),
        strokeWidth: featureIcon.getAttribute('stroke-width'),
      }

      unmountFeatures()

      render(<HowItWorksSection />)

      const stepIcon = screen.getByTestId('step-icon-1').querySelector('svg')

      // Both sections should use the same icon styling conventions
      expect(stepIcon).toHaveAttribute('viewBox', featureIconAttrs.viewBox)
      expect(stepIcon).toHaveAttribute('fill', featureIconAttrs.fill)
      expect(stepIcon).toHaveAttribute('stroke', featureIconAttrs.stroke)
      expect(stepIcon).toHaveAttribute('stroke-width', featureIconAttrs.strokeWidth)
    })
  })

  describe('Modern Brand Reinforcement', () => {
    it('should use rounded path caps for modern aesthetic', () => {
      render(<FeaturesSection />)

      const icons = [
        screen.getByTestId('url-shortening-icon'),
        screen.getByTestId('analytics-icon'),
        screen.getByTestId('link-management-icon'),
      ]

      icons.forEach((icon) => {
        const path = icon.querySelector('path')
        expect(path).toHaveAttribute('stroke-linecap', 'round')
        expect(path).toHaveAttribute('stroke-linejoin', 'round')
      })
    })

    it('should position icons prominently at top of feature cards', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId(/^feature-card-/)

      featureCards.forEach((card) => {
        // Icon container should have margin-bottom for proper spacing
        const iconContainer = card.querySelector('.text-primary')
        expect(iconContainer).toHaveClass('mb-4')
      })
    })

    it('should style step icons with primary brand color', () => {
      render(<HowItWorksSection />)

      const stepIcons = [
        screen.getByTestId('step-icon-1'),
        screen.getByTestId('step-icon-2'),
        screen.getByTestId('step-icon-3'),
      ]

      stepIcons.forEach((iconContainer) => {
        expect(iconContainer).toHaveClass('text-primary')
      })
    })
  })
})
