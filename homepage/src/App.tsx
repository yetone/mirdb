import { Hero } from './components/sections/Hero'
import { Header, Footer } from './components/layout'
import { Features } from './components/sections/Features'
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
