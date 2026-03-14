import { Hero } from './components/sections/Hero'
import { Header } from './components/layout'
import { Features } from './components/sections/Features'
import { SocialProof } from './components/sections/SocialProof'
import { CTASection, ContactSalesCTA, LearnMoreCTA } from './components/sections/CTASection'
import { NAV_LINKS } from './utils/constants'

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

        {/* Learn More CTA after Features */}
        <div className="container-main py-8 text-center" id="learn-more-section">
          <LearnMoreCTA
            text="Learn More"
            href="#features"
            variant="button"
          />
        </div>

        <SocialProof />

        {/* Contact Sales CTA after Social Proof */}
        <div className="container-main py-12" id="contact-sales-section">
          <ContactSalesCTA
            title="Need a Custom Solution?"
            description="Our sales team is here to help you find the perfect plan for your organization."
            buttonText="Contact Sales"
            href="#contact"
          />
        </div>

        {/* Main CTA Section before footer */}
        <CTASection
          title="Ready to Transform Your Workflow?"
          description="Join thousands of teams already using our platform. Start your free trial today and experience the difference."
          primaryCTA={{
            label: "Start Free Trial",
            href: "#signup",
            variant: "primary",
            size: "lg"
          }}
          secondaryCTA={{
            label: "Schedule Demo",
            href: "#demo",
            variant: "outline",
            size: "lg"
          }}
          background="gradient"
        />
      </main>
    </>
  )
}

export default App
