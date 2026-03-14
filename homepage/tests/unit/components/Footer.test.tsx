/**
 * Unit tests for Footer component.
 * Owner: Scenario 6 - Footer Section Implementation
 *
 * Test cases:
 * - Footer displays with multi-column navigation layout
 * - Footer contains categorized links (Product, Company, Legal, etc.)
 * - Copyright notice present with year 2026
 * - Social icons present with correct hrefs and aria-labels
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Footer, FooterColumn } from '../../../src/components/layout/Footer'
import type { SocialLink, NavLink } from '../../../src/types'

const mockColumns: FooterColumn[] = [
  {
    title: 'Product',
    links: [
      { label: 'Features', href: '/features' },
      { label: 'Pricing', href: '/pricing' },
      { label: 'Documentation', href: '/docs' },
    ],
  },
  {
    title: 'Company',
    links: [
      { label: 'About', href: '/about' },
      { label: 'Blog', href: '/blog' },
      { label: 'Careers', href: '/careers' },
    ],
  },
  {
    title: 'Legal',
    links: [
      { label: 'Privacy Policy', href: '/privacy' },
      { label: 'Terms of Service', href: '/terms' },
    ],
  },
  {
    title: 'Support',
    links: [
      { label: 'Help Center', href: '/help' },
      { label: 'Contact Us', href: '/contact' },
    ],
  },
]

const mockSocialLinks: SocialLink[] = [
  { platform: 'Twitter', href: 'https://twitter.com/company', icon: 'twitter' },
  { platform: 'LinkedIn', href: 'https://linkedin.com/company/company', icon: 'linkedin' },
  { platform: 'GitHub', href: 'https://github.com/company', icon: 'github' },
]

const mockLegalLinks: NavLink[] = [
  { label: 'Privacy Policy', href: '/privacy' },
  { label: 'Terms of Service', href: '/terms' },
]

const mockCompanyName = 'TestCompany'

describe('Footer Component', () => {
  // Test Case 1: Footer displays with multi-column navigation layout
  it('should render footer with multi-column navigation layout', () => {
    render(
      <Footer
        columns={mockColumns}
        socialLinks={mockSocialLinks}
        legalLinks={mockLegalLinks}
        companyName={mockCompanyName}
      />
    )

    // Check footer exists
    const footer = screen.getByTestId('footer')
    expect(footer).toBeInTheDocument()

    // Check all columns are rendered
    mockColumns.forEach((_, index) => {
      expect(screen.getByTestId(`footer-column-${index}`)).toBeInTheDocument()
    })
  })

  // Test Case 2: Footer contains categorized links (Product, Company, Legal, etc.)
  it('should render categorized navigation links in columns', () => {
    render(
      <Footer
        columns={mockColumns}
        socialLinks={mockSocialLinks}
        legalLinks={mockLegalLinks}
        companyName={mockCompanyName}
      />
    )

    // Check column titles
    expect(screen.getByText('Product')).toBeInTheDocument()
    expect(screen.getByText('Company')).toBeInTheDocument()
    expect(screen.getByText('Legal')).toBeInTheDocument()
    expect(screen.getByText('Support')).toBeInTheDocument()

    // Check individual links
    expect(screen.getByText('Features')).toBeInTheDocument()
    expect(screen.getByText('Pricing')).toBeInTheDocument()
    expect(screen.getByText('About')).toBeInTheDocument()
    expect(screen.getByText('Careers')).toBeInTheDocument()
    expect(screen.getByText('Help Center')).toBeInTheDocument()
  })

  // Test Case 3: Copyright notice present with year 2026
  it('should display copyright notice with year 2026', () => {
    render(
      <Footer
        columns={mockColumns}
        socialLinks={mockSocialLinks}
        legalLinks={mockLegalLinks}
        companyName={mockCompanyName}
      />
    )

    const copyright = screen.getByTestId('copyright-notice')
    expect(copyright).toBeInTheDocument()
    expect(copyright).toHaveTextContent('2026')
    expect(copyright).toHaveTextContent(mockCompanyName)
    expect(copyright).toHaveTextContent('All rights reserved')
  })

  // Test Case 4: Social icons present with correct hrefs and aria-labels
  it('should render social media icons with correct hrefs and aria-labels', () => {
    render(
      <Footer
        columns={mockColumns}
        socialLinks={mockSocialLinks}
        legalLinks={mockLegalLinks}
        companyName={mockCompanyName}
      />
    )

    // Check social links container exists
    const socialLinksContainer = screen.getByTestId('social-links')
    expect(socialLinksContainer).toBeInTheDocument()

    // Check each social link
    const twitterLink = screen.getByTestId('social-link-twitter')
    expect(twitterLink).toBeInTheDocument()
    expect(twitterLink).toHaveAttribute('href', 'https://twitter.com/company')
    expect(twitterLink).toHaveAttribute('aria-label', 'Follow us on Twitter')

    const linkedinLink = screen.getByTestId('social-link-linkedin')
    expect(linkedinLink).toBeInTheDocument()
    expect(linkedinLink).toHaveAttribute('href', 'https://linkedin.com/company/company')
    expect(linkedinLink).toHaveAttribute('aria-label', 'Follow us on LinkedIn')

    const githubLink = screen.getByTestId('social-link-github')
    expect(githubLink).toBeInTheDocument()
    expect(githubLink).toHaveAttribute('href', 'https://github.com/company')
    expect(githubLink).toHaveAttribute('aria-label', 'Follow us on GitHub')
  })

  it('should render with contentinfo role for accessibility', () => {
    render(
      <Footer
        columns={mockColumns}
        socialLinks={mockSocialLinks}
        legalLinks={mockLegalLinks}
        companyName={mockCompanyName}
      />
    )

    expect(screen.getByRole('contentinfo')).toBeInTheDocument()
  })

  it('should render legal links with correct hrefs', () => {
    render(
      <Footer
        columns={mockColumns}
        socialLinks={mockSocialLinks}
        legalLinks={mockLegalLinks}
        companyName={mockCompanyName}
      />
    )

    const legalLinksContainer = screen.getByTestId('legal-links')
    expect(legalLinksContainer).toBeInTheDocument()

    const privacyLink = screen.getByTestId('legal-link-privacy-policy')
    expect(privacyLink).toBeInTheDocument()
    expect(privacyLink).toHaveAttribute('href', '/privacy')

    const termsLink = screen.getByTestId('legal-link-terms-of-service')
    expect(termsLink).toBeInTheDocument()
    expect(termsLink).toHaveAttribute('href', '/terms')
  })

  it('should not render legal links section when legalLinks is empty', () => {
    render(
      <Footer
        columns={mockColumns}
        socialLinks={mockSocialLinks}
        legalLinks={[]}
        companyName={mockCompanyName}
      />
    )

    expect(screen.queryByTestId('legal-links')).not.toBeInTheDocument()
  })

  it('should not render legal links section when legalLinks is not provided', () => {
    render(
      <Footer
        columns={mockColumns}
        socialLinks={mockSocialLinks}
        companyName={mockCompanyName}
      />
    )

    expect(screen.queryByTestId('legal-links')).not.toBeInTheDocument()
  })

  it('should render navigation links with correct href attributes', () => {
    render(
      <Footer
        columns={mockColumns}
        socialLinks={mockSocialLinks}
        legalLinks={mockLegalLinks}
        companyName={mockCompanyName}
      />
    )

    // Check specific links have correct hrefs
    const featuresLink = screen.getByTestId('footer-link-features')
    expect(featuresLink).toHaveAttribute('href', '/features')

    const pricingLink = screen.getByTestId('footer-link-pricing')
    expect(pricingLink).toHaveAttribute('href', '/pricing')

    const aboutLink = screen.getByTestId('footer-link-about')
    expect(aboutLink).toHaveAttribute('href', '/about')
  })

  it('should render external links with target="_blank" and rel="noopener noreferrer"', () => {
    const columnsWithExternalLink: FooterColumn[] = [
      {
        title: 'Resources',
        links: [
          { label: 'External Docs', href: 'https://docs.example.com', isExternal: true },
        ],
      },
    ]

    render(
      <Footer
        columns={columnsWithExternalLink}
        socialLinks={mockSocialLinks}
        companyName={mockCompanyName}
      />
    )

    const externalLink = screen.getByTestId('footer-link-external-docs')
    expect(externalLink).toHaveAttribute('target', '_blank')
    expect(externalLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('should render social links with target="_blank" and rel="noopener noreferrer"', () => {
    render(
      <Footer
        columns={mockColumns}
        socialLinks={mockSocialLinks}
        companyName={mockCompanyName}
      />
    )

    mockSocialLinks.forEach((social) => {
      const link = screen.getByTestId(`social-link-${social.platform.toLowerCase()}`)
      expect(link).toHaveAttribute('target', '_blank')
      expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    })
  })
})
