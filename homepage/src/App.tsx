import { Hero } from './components/sections/Hero'
import { Header, Footer } from './components/layout'
import { Features } from './components/sections/Features'
import { SocialProof } from './components/sections/SocialProof'
import { CTASection } from './components/sections/CTASection'
import { NAV_LINKS, SOCIAL_LINKS, FOOTER_COLUMNS, LEGAL_LINKS } from './utils/constants'

function App() {
  return (
    <>
      <Header
        productName="ProductName"
        tagline="Your workflow, simplified"
        logo="/logo.svg"
        navLinks={NAV_LINKS}
        ctaButton={{
          label: "Get Started",
          href: "#signup",
          variant: "primary",
          size: "md"
        }}
      />
      <main className="pt-16 md:pt-20">
        <Hero
          headline="Transform Your Workflow with Intelligent Automation"
          subheadline="Our platform helps teams work smarter, not harder. Automate repetitive tasks, streamline collaboration, and unlock your team's full potential with AI-powered tools designed for modern workplaces."
          ctaButton={{
            label: "Get Started Free",
            href: "#signup",
            variant: "primary",
            size: "lg"
          }}
        />
        <Features />
        <SocialProof />
        <CTASection
          title="Ready to Get Started?"
          description="Join thousands of teams already using our platform to transform their workflow. Start your free trial today."
          primaryCTA={{
            label: "Start Free Trial",
            href: "#signup",
            variant: "primary",
            size: "lg"
          }}
          secondaryCTA={{
            label: "Learn More",
            href: "#features",
            variant: "outline",
            size: "lg"
          }}
        />
      </main>
      <Footer
        columns={FOOTER_COLUMNS}
        socialLinks={SOCIAL_LINKS}
        legalLinks={LEGAL_LINKS}
        companyName="ProductName"
      />
    </>
  )
}

export default App
