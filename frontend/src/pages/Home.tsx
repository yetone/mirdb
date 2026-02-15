/**
 * Homepage Component
 * Owner: Scenario 1 - Homepage Structure and Layout
 *
 * Main landing page for the URL Shortening Service.
 * Displays hero section, quick shorten form, features, and how-it-works sections.
 *
 * Requirements covered:
 * - REQ-1: Homepage at root path (/)
 * - REQ-2: Hero section with value proposition
 * - REQ-4: Features section
 * - REQ-5: How it Works section
 * - REQ-8: Responsive design
 */

import { useNavigate } from 'react-router-dom';
import Navbar from '../components/Navbar';
import HeroSection from '../components/home/HeroSection';
import { QuickShortenForm } from '../components/home/QuickShortenForm';
import FeaturesSection from '../components/home/FeaturesSection';
import HowItWorksSection from '../components/home/HowItWorksSection';
import Footer from '../components/home/Footer';

export default function Home() {
  const navigate = useNavigate();

  const handleGetStarted = () => {
    navigate('/register');
  };

  const handleLearnMore = () => {
    const featuresSection = document.getElementById('features');
    if (featuresSection) {
      featuresSection.scrollIntoView({ behavior: 'smooth' });
    }
  };

  return (
    <div className="min-h-screen flex flex-col" data-testid="home-page">
      <Navbar />
      <main className="flex-1">
        <HeroSection onGetStarted={handleGetStarted} onLearnMore={handleLearnMore} />

        {/* QuickShortenForm - owned by Scenario 2 */}
        <div id="quick-shorten" className="py-8 bg-base-100">
          <div className="container mx-auto px-4">
            <QuickShortenForm />
          </div>
        </div>

        <div id="features">
          <FeaturesSection />
        </div>

        <div id="how-it-works">
          <HowItWorksSection />
        </div>
      </main>

      <Footer />
    </div>
  );
}
