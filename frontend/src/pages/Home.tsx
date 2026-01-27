/**
 * Homepage Container Component
 * Owner: Scenarios 2, 3 - Navigation, Scenario 16 - Route Config
 *
 * Composes all homepage sections:
 * - HeroSection
 * - FeaturesSection
 * - HowItWorksSection
 * - CTASection
 * - Footer
 */
import Navbar from '../components/Navbar';
import {
  HeroSection,
  FeaturesSection,
  HowItWorksSection,
  CTASection,
  Footer,
} from '../components/homepage';

export default function Home() {
  return (
    <div className="min-h-screen bg-base-100">
      <Navbar />
      <main>
        <HeroSection />
        <FeaturesSection />
        <HowItWorksSection />
        <CTASection />
      </main>
      <Footer />
    </div>
  );
}
