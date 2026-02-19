/**
 * Hero Section Tests
 * Owner: Scenario 1 - Hero Section & Value Proposition
 */
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { Hero } from './Hero'

describe('Hero', () => {
  beforeEach(() => {
    // Reset DOM before each test
    document.body.innerHTML = ''
  })

  describe('Rendering', () => {
    it('renders without errors', () => {
      render(<Hero />)
      expect(screen.getByRole('region', { name: /mirdb/i })).toBeInTheDocument()
    })

    it('renders the hero section with correct id', () => {
      const { container } = render(<Hero />)
      const section = container.querySelector('#hero')
      expect(section).toBeInTheDocument()
    })
  })

  describe('Logo Display', () => {
    it('displays the MirDB logo', () => {
      render(<Hero />)
      const logo = screen.getByAltText('MirDB Logo')
      expect(logo).toBeInTheDocument()
      expect(logo).toHaveAttribute('src', '/assets/logo.gif')
    })

    it('logo has appropriate dimensions', () => {
      render(<Hero />)
      const logo = screen.getByAltText('MirDB Logo')
      expect(logo).toHaveAttribute('width', '200')
      expect(logo).toHaveAttribute('height', '200')
    })
  })

  describe('Product Name and Tagline', () => {
    it('displays h1 element with MirDB text', () => {
      render(<Hero />)
      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent('MirDB')
    })

    it('displays tagline describing Memcached compatibility', () => {
      render(<Hero />)
      // Use getAllByText since both tagline and description contain the search text
      const elements = screen.getAllByText(/persistent key-value store/i)
      expect(elements.length).toBeGreaterThanOrEqual(1)

      // Check tagline specifically contains Memcached
      const tagline = screen.getByText(/^A persistent key-value store with Memcached protocol compatibility$/i)
      expect(tagline).toBeInTheDocument()
    })

    it('heading has correct id for accessibility', () => {
      render(<Hero />)
      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading).toHaveAttribute('id', 'hero-title')
    })
  })

  describe('Get Started CTA', () => {
    it('renders Get Started button', () => {
      render(<Hero />)
      const button = screen.getByRole('button', { name: /get started/i })
      expect(button).toBeInTheDocument()
    })

    it('Get Started button has primary variant styling', () => {
      render(<Hero />)
      const button = screen.getByRole('button', { name: /get started/i })
      expect(button.className).toMatch(/primary/i)
    })

    it('Get Started button scrolls to quick-start section when clicked', () => {
      // Create a mock quick-start section
      const quickStartSection = document.createElement('section')
      quickStartSection.id = 'quick-start'
      quickStartSection.scrollIntoView = vi.fn()
      document.body.appendChild(quickStartSection)

      render(<Hero />)
      const button = screen.getByRole('button', { name: /get started/i })
      fireEvent.click(button)

      expect(quickStartSection.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' })
    })

    it('Get Started button has accessible label', () => {
      render(<Hero />)
      const button = screen.getByRole('button', { name: /get started/i })
      expect(button).toHaveAttribute('aria-label', 'Get started with MirDB')
    })
  })

  describe('GitHub CTA', () => {
    it('renders View on GitHub link', () => {
      render(<Hero />)
      const link = screen.getByRole('link', { name: /view.*github/i })
      expect(link).toBeInTheDocument()
    })

    it('GitHub link has correct href', () => {
      render(<Hero />)
      const link = screen.getByRole('link', { name: /view.*github/i })
      expect(link).toHaveAttribute('href', 'https://github.com/yetone/mirdb')
    })

    it('GitHub link opens in new tab', () => {
      render(<Hero />)
      const link = screen.getByRole('link', { name: /view.*github/i })
      expect(link).toHaveAttribute('target', '_blank')
    })

    it('GitHub link has security attributes for external link', () => {
      render(<Hero />)
      const link = screen.getByRole('link', { name: /view.*github/i })
      expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('GitHub link has secondary variant styling', () => {
      render(<Hero />)
      const link = screen.getByRole('link', { name: /view.*github/i })
      expect(link.className).toMatch(/secondary/i)
    })

    it('GitHub link has accessible label', () => {
      render(<Hero />)
      const link = screen.getByRole('link', { name: /view.*github/i })
      expect(link).toHaveAttribute('aria-label', 'View MirDB on GitHub')
    })
  })

  describe('Accessibility', () => {
    it('hero section has aria-labelledby pointing to heading', () => {
      render(<Hero />)
      const section = screen.getByRole('region')
      expect(section).toHaveAttribute('aria-labelledby', 'hero-title')
    })

    it('GitHub icon is hidden from screen readers', () => {
      render(<Hero />)
      const link = screen.getByRole('link', { name: /view.*github/i })
      const svg = link.querySelector('svg')
      expect(svg).toHaveAttribute('aria-hidden', 'true')
    })
  })

  describe('Snapshot', () => {
    it('matches snapshot', () => {
      const { container } = render(<Hero />)
      expect(container.firstChild).toMatchSnapshot()
    })
  })
})
