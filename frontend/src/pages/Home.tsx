/**
 * Home Page Component
 * Owner: Scenario 1 - Homepage Hero Section Rendering (primary)
 *        All other scenarios may extend this file
 *
 * Main homepage composing all sections: Navbar, HeroSection,
 * FeaturesSection, Footer, and BackgroundEffect.
 *
 * Expected exports:
 * - Home: React.FC component
 */
import HeroSection from '../components/Home/HeroSection';
import UrlShortenerForm from '../components/Home/UrlShortenerForm';
import FeaturesSection from '../components/Home/FeaturesSection';

export default function Home() {
  return (
    <main data-testid="home-page">
      <HeroSection urlShortenerForm={<UrlShortenerForm />} />
      <FeaturesSection />
    </main>
  );
}
