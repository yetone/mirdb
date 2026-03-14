import { Hero } from './components/sections/Hero'
import { Features } from './components/sections/Features'

function App() {
  return (
    <main>
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
  )
}

export default App
