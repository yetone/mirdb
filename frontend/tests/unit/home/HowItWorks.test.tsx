/**
 * How It Works Section Unit Tests
 * Owner: Scenario 5 - How It Works Section
 *
 * Tests for HowItWorks component:
 * - Contains section heading
 * - Displays 3 numbered/ordered steps
 * - Steps explain: create, share, track flow
 *
 * Testing framework: Vitest + @testing-library/react
 */
import React from 'react'
import { describe, it, expect } from 'vitest'
import { screen, within } from '@testing-library/react'
import { renderWithProviders } from '../../utils/renderWithProviders'
import { HowItWorks } from '@/components/home/HowItWorks'

describe('HowItWorks', () => {
  describe('Test Case 1: Section with heading containing "How It Works" exists', () => {
    it('should render a How It Works section', () => {
      renderWithProviders(<HowItWorks />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section).toBeInTheDocument()
    })

    it('should render the section as a semantic section element', () => {
      renderWithProviders(<HowItWorks />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section.tagName.toLowerCase()).toBe('section')
    })

    it('should have a heading containing "How It Works"', () => {
      renderWithProviders(<HowItWorks />)

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toBeInTheDocument()
      expect(heading.textContent?.toLowerCase()).toContain('how it works')
    })

    it('should have proper ARIA labeling', () => {
      renderWithProviders(<HowItWorks />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading')

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toHaveAttribute('id', 'how-it-works-heading')
    })
  })

  describe('Test Case 2: At least 3 visually distinct steps with numbers or icons', () => {
    it('should render exactly 3 steps', () => {
      renderWithProviders(<HowItWorks />)

      const step1 = screen.getByTestId('step-1')
      const step2 = screen.getByTestId('step-2')
      const step3 = screen.getByTestId('step-3')

      expect(step1).toBeInTheDocument()
      expect(step2).toBeInTheDocument()
      expect(step3).toBeInTheDocument()
    })

    it('should render steps in an ordered list', () => {
      renderWithProviders(<HowItWorks />)

      const stepsList = screen.getByRole('list', { name: /steps/i })
      expect(stepsList).toBeInTheDocument()
      expect(stepsList.tagName.toLowerCase()).toBe('ol')
    })

    it('should have visual step numbers displayed', () => {
      renderWithProviders(<HowItWorks />)

      const stepNumber1 = screen.getByTestId('step-number-1')
      const stepNumber2 = screen.getByTestId('step-number-2')
      const stepNumber3 = screen.getByTestId('step-number-3')

      expect(stepNumber1).toHaveTextContent('1')
      expect(stepNumber2).toHaveTextContent('2')
      expect(stepNumber3).toHaveTextContent('3')
    })

    it('should have icons for each step', () => {
      renderWithProviders(<HowItWorks />)

      const icon1 = screen.getByTestId('step-icon-1')
      const icon2 = screen.getByTestId('step-icon-2')
      const icon3 = screen.getByTestId('step-icon-3')

      expect(icon1).toBeInTheDocument()
      expect(icon2).toBeInTheDocument()
      expect(icon3).toBeInTheDocument()

      // Each icon container should contain an SVG
      expect(icon1.querySelector('svg')).toBeInTheDocument()
      expect(icon2.querySelector('svg')).toBeInTheDocument()
      expect(icon3.querySelector('svg')).toBeInTheDocument()
    })

    it('should render steps as list items', () => {
      renderWithProviders(<HowItWorks />)

      const listItems = screen.getAllByRole('listitem')
      expect(listItems).toHaveLength(3)
    })
  })

  describe('Test Case 3: Steps mention create/paste URL, share/get short link, track/analytics', () => {
    it('should have step 1 mention creating or pasting a URL', () => {
      renderWithProviders(<HowItWorks />)

      const step1 = screen.getByTestId('step-1')
      const stepContent = step1.textContent?.toLowerCase() || ''

      // Step 1 should mention paste/create and URL
      expect(stepContent).toMatch(/paste|create|enter/)
      expect(stepContent).toMatch(/url|link/)
    })

    it('should have step 2 mention getting or sharing a short link', () => {
      renderWithProviders(<HowItWorks />)

      const step2 = screen.getByTestId('step-2')
      const stepContent = step2.textContent?.toLowerCase() || ''

      // Step 2 should mention short link and share/copy
      expect(stepContent).toMatch(/short|shareable|compact/)
      expect(stepContent).toMatch(/link|share|copy/)
    })

    it('should have step 3 mention tracking or analytics', () => {
      renderWithProviders(<HowItWorks />)

      const step3 = screen.getByTestId('step-3')
      const stepContent = step3.textContent?.toLowerCase() || ''

      // Step 3 should mention tracking/analytics
      expect(stepContent).toMatch(/track|analytics|monitor|performance/)
    })
  })

  describe('Test Case 4: Steps are visually numbered (1, 2, 3) or use distinct icons', () => {
    it('should display visible number badges for each step', () => {
      renderWithProviders(<HowItWorks />)

      const stepNumber1 = screen.getByTestId('step-number-1')
      const stepNumber2 = screen.getByTestId('step-number-2')
      const stepNumber3 = screen.getByTestId('step-number-3')

      // Numbers should be visible in the document
      expect(stepNumber1).toBeVisible()
      expect(stepNumber2).toBeVisible()
      expect(stepNumber3).toBeVisible()
    })

    it('should have distinct icons for each step', () => {
      renderWithProviders(<HowItWorks />)

      const icon1 = screen.getByTestId('step-icon-1').querySelector('svg')
      const icon2 = screen.getByTestId('step-icon-2').querySelector('svg')
      const icon3 = screen.getByTestId('step-icon-3').querySelector('svg')

      // Each icon should have different path data (distinct icons)
      const path1 = icon1?.querySelector('path')?.getAttribute('d')
      const path2 = icon2?.querySelector('path')?.getAttribute('d')
      const path3 = icon3?.querySelector('path')?.getAttribute('d')

      expect(path1).not.toBe(path2)
      expect(path2).not.toBe(path3)
      expect(path1).not.toBe(path3)
    })

    it('should have clear visual hierarchy with headings for each step', () => {
      renderWithProviders(<HowItWorks />)

      const step1 = screen.getByTestId('step-1')
      const step2 = screen.getByTestId('step-2')
      const step3 = screen.getByTestId('step-3')

      // Each step should have an h3 heading
      const heading1 = within(step1).getByRole('heading', { level: 3 })
      const heading2 = within(step2).getByRole('heading', { level: 3 })
      const heading3 = within(step3).getByRole('heading', { level: 3 })

      expect(heading1).toBeInTheDocument()
      expect(heading2).toBeInTheDocument()
      expect(heading3).toBeInTheDocument()
    })
  })

  describe('Additional structure and content tests', () => {
    it('should have a title and description for each step', () => {
      renderWithProviders(<HowItWorks />)

      const steps = screen.getAllByRole('listitem')

      steps.forEach(step => {
        // Each step should have a heading (title)
        const heading = within(step).getByRole('heading', { level: 3 })
        expect(heading).toBeInTheDocument()
        expect(heading.textContent?.length).toBeGreaterThan(0)

        // Each step should have a paragraph (description)
        const paragraph = step.querySelector('p')
        expect(paragraph).toBeInTheDocument()
        expect(paragraph?.textContent?.length).toBeGreaterThan(0)
      })
    })

    it('should have accessible icons with aria-hidden', () => {
      renderWithProviders(<HowItWorks />)

      const icons = [
        screen.getByTestId('step-icon-1').querySelector('svg'),
        screen.getByTestId('step-icon-2').querySelector('svg'),
        screen.getByTestId('step-icon-3').querySelector('svg'),
      ]

      icons.forEach(icon => {
        expect(icon).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })
})
