import { Link, Globe, BarChart3, Share2 } from 'lucide-react';
import { Link as RouterLink, useNavigate } from 'react-router-dom';
import GlassMorphismCard from '../components/GlassMorphismCard';
import FuturisticButton from '../components/FuturisticButton';
import Navbar from '../components/Navbar';

const features = [
  {
    id: 'url-shortening',
    title: 'URL Shortening',
    description: 'Create memorable short links in seconds. Transform long URLs into clean, shareable links.',
    icon: <Link className="w-6 h-6" />,
  },
  {
    id: 'analytics-dashboard',
    title: 'Analytics Dashboard',
    description: 'Track clicks and performance with detailed analytics. See how your links are performing in real-time.',
    icon: <BarChart3 className="w-6 h-6" />,
  },
  {
    id: 'geographic-insights',
    title: 'Geographic Insights',
    description: 'Know where your audience is. Get detailed geographic data about your link visitors.',
    icon: <Globe className="w-6 h-6" />,
  },
  {
    id: 'share-collaborate',
    title: 'Share & Collaborate',
    description: 'Share public stats with your team. Collaborate and make data-driven decisions together.',
    icon: <Share2 className="w-6 h-6" />,
  },
];

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
    <div className="min-h-screen bg-base-200">
      <Navbar variant="transparent" />

      {/* Hero Section */}
      <section className="hero min-h-[60vh] bg-gradient-to-br from-primary/20 to-secondary/20" data-testid="hero-section">
        <div className="hero-content text-center">
          <div className="max-w-2xl">
            <h1 className="text-5xl font-bold mb-6">
              Shorten, Share, Track
            </h1>
            <p className="text-xl mb-8 text-base-content/70">
              Create short, memorable links in seconds. Track clicks, analyze your audience, and optimize your marketing with our powerful URL shortening service.
            </p>
            <div className="flex gap-4 justify-center">
              <FuturisticButton variant="primary" size="lg" onClick={handleGetStarted}>
                Get Started Free
              </FuturisticButton>
              <FuturisticButton variant="outline" size="lg" onClick={handleLearnMore}>
                Learn More
              </FuturisticButton>
            </div>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section id="features" className="py-20 px-4 bg-base-100" data-testid="features-section">
        <div className="max-w-6xl mx-auto">
          <h2 className="text-3xl font-bold text-center mb-12">
            Why Choose URLShort?
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6" data-testid="features-grid">
            {features.map((feature) => (
              <GlassMorphismCard
                key={feature.id}
                data-testid={`feature-card-${feature.id}`}
                title={feature.title}
                description={feature.description}
                icon={feature.icon}
              >
                <></>
              </GlassMorphismCard>
            ))}
          </div>
        </div>
      </section>

      {/* How It Works Section */}
      <section id="how-it-works" className="py-20 px-4 bg-base-200">
        <div className="max-w-4xl mx-auto">
          <h2 className="text-3xl font-bold text-center mb-12">
            How It Works
          </h2>
          <div className="flex flex-col md:flex-row gap-8 justify-center">
            <div className="flex flex-col items-center text-center">
              <div className="w-16 h-16 rounded-full bg-primary text-primary-content flex items-center justify-center text-2xl font-bold mb-4">
                1
              </div>
              <h3 className="text-lg font-semibold mb-2">Paste your long URL</h3>
              <p className="text-base-content/70">Enter any long URL you want to shorten</p>
            </div>
            <div className="flex flex-col items-center text-center">
              <div className="w-16 h-16 rounded-full bg-primary text-primary-content flex items-center justify-center text-2xl font-bold mb-4">
                2
              </div>
              <h3 className="text-lg font-semibold mb-2">Get your short link</h3>
              <p className="text-base-content/70">Receive a compact, shareable URL instantly</p>
            </div>
            <div className="flex flex-col items-center text-center">
              <div className="w-16 h-16 rounded-full bg-primary text-primary-content flex items-center justify-center text-2xl font-bold mb-4">
                3
              </div>
              <h3 className="text-lg font-semibold mb-2">Track performance</h3>
              <p className="text-base-content/70">Monitor clicks and analytics over time</p>
            </div>
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-20 px-4 bg-primary text-primary-content">
        <div className="max-w-2xl mx-auto text-center">
          <h2 className="text-3xl font-bold mb-4">
            Ready to get started?
          </h2>
          <p className="text-lg mb-8 opacity-80">
            Join thousands of users who trust URLShort for their link management needs.
          </p>
          <RouterLink to="/register" className="btn btn-secondary btn-lg">
            Sign Up Free
          </RouterLink>
        </div>
      </section>

      {/* Footer */}
      <footer className="footer footer-center p-10 bg-base-300 text-base-content">
        <div>
          <p className="font-bold text-lg">URLShort</p>
          <p>Shorten, Share, Track</p>
        </div>
        <div className="grid grid-flow-col gap-4">
          <a href="/privacy" className="link link-hover">Privacy Policy</a>
          <a href="/terms" className="link link-hover">Terms of Service</a>
          <a href="/contact" className="link link-hover">Contact</a>
        </div>
        <div>
          <p>&copy; {new Date().getFullYear()} URLShort. All rights reserved.</p>
        </div>
      </footer>
    </div>
  );
}
