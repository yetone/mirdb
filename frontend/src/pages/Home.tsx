import { HeroSection } from '../components/homepage'
import { BackgroundEffect } from '../components/BackgroundEffect'

export function Home() {
  const handleLearnMore = () => {
    // Will scroll to features section when implemented
    const featuresSection = document.getElementById('features')
    if (featuresSection) {
      featuresSection.scrollIntoView({ behavior: 'smooth' })
    }
  }

  return (
    <main className="min-h-screen relative">
      <BackgroundEffect />
      <HeroSection onLearnMoreClick={handleLearnMore} />
      {/* Other sections will be added by their respective scenario builders */}
      <section id="features" className="py-20">
        {/* FeaturesSection placeholder */}
      </section>
    </main>
  )
}

export default Home
