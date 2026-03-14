/**
 * Main Homepage component assembling all sections.
 * Owner: Scenario 10 - SEO and Semantic HTML
 *
 * Requirements:
 * - Semantic HTML structure (header, main, section, footer) (NFR-2)
 * - Proper heading hierarchy (h1 > h2 > h3)
 * - Meta tags for SEO
 * - Open Graph tags for social sharing
 *
 * Structure:
 * - Header (sticky)
 * - main
 *   - Hero section
 *   - Features section
 *   - Social Proof section
 *   - CTA section
 * - Footer
 */

import { Hero } from '../components/sections/Hero'
import { Header, Footer } from '../components/layout'
import { Features } from '../components/sections/Features'
import { SocialProof } from '../components/sections/SocialProof'
import { CTASection } from '../components/sections/CTASection'
import {
  NAV_LINKS,
  SOCIAL_LINKS,
  FOOTER_COLUMNS,
  LEGAL_LINKS,
} from '../utils/constants'

export interface HomepageProps {
  productName?: string
  tagline?: string
  logo?: string
}

export function Homepage({
  productName = 'ProductName',
  tagline = 'Your workflow, simplified',
  logo = '/logo.svg',
}: HomepageProps) {
  return (
    <>
      {/* Semantic header element with navigation */}
      <Header
        productName={productName}
        tagline={tagline}
        logo={logo}
        navLinks={NAV_LINKS}
        ctaButton={{
          label: 'Get Started',
          href: '#signup',
          variant: 'primary',
          size: 'md',
        }}
      />

      {/* Semantic main element wrapping primary content */}
      <main className="pt-16 md:pt-20" role="main" id="main-content">
        {/* Hero section with h1 headline */}
        <Hero
          headline="Transform Your Workflow with Intelligent Automation"
          subheadline="Our platform helps teams work smarter, not harder. Automate repetitive tasks, streamline collaboration, and unlock your team's full potential with AI-powered tools designed for modern workplaces."
          ctaButton={{
            label: 'Get Started Free',
            href: '#signup',
            variant: 'primary',
            size: 'lg',
          }}
        />

        {/* Features section with h2 heading */}
        <Features />

        {/* Social proof section with h2 heading */}
        <SocialProof />

        {/* CTA section with h2 heading */}
        <CTASection
          title="Ready to Get Started?"
          description="Join thousands of teams already using our platform to transform their workflow. Start your free trial today."
          primaryCTA={{
            label: 'Start Free Trial',
            href: '#signup',
            variant: 'primary',
            size: 'lg',
          }}
          secondaryCTA={{
            label: 'Learn More',
            href: '#features',
            variant: 'outline',
            size: 'lg',
          }}
        />
      </main>

      {/* Semantic footer element */}
      <Footer
        columns={FOOTER_COLUMNS}
        socialLinks={SOCIAL_LINKS}
        legalLinks={LEGAL_LINKS}
        companyName={productName}
      />
    </>
  )
}

export default Homepage
