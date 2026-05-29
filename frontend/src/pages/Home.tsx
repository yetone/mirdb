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
import Footer from '../components/Home/Footer';

export default function Home() {
  return (
    <>
      <a href="#main-content" className="skip-link">
        Skip to main content
      </a>
      <main data-testid="home-page" id="main-content" className="home-page min-h-screen flex flex-col">
        <HeroSection urlShortenerForm={<UrlShortenerForm />} />
        <FeaturesSection />
        <Footer />
      </main>
    </>
  );
}
