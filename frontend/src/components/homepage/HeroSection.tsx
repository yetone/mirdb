/**
 * Hero Section Component.
 * Owner: Scenario 1 - Hero Section & Value Proposition
 *
 * Expected behavior:
 * - Display compelling headline with product tagline
 * - Show subheadline explaining value proposition
 * - Primary CTA button ("Get Started Free") linking to /register
 * - Secondary CTA ("Learn More") for scroll or navigation
 * - Visual demo element on desktop (right side split layout)
 * - Responsive: stacked on mobile, split on desktop
 */

import { useNavigate } from 'react-router-dom';
import { HERO_CONTENT } from '../../utils/constants';

interface HeroSectionProps {
  onGetStarted?: () => void;
  onLearnMore?: () => void;
}

function HeroSection({ onGetStarted, onLearnMore }: HeroSectionProps) {
  const navigate = useNavigate();

  const handleGetStarted = () => {
    if (onGetStarted) {
      onGetStarted();
    }
    navigate('/register');
  };

  const handleLearnMore = () => {
    if (onLearnMore) {
      onLearnMore();
    }
    const featuresSection = document.getElementById('features');
    if (featuresSection) {
      featuresSection.scrollIntoView({ behavior: 'smooth' });
    }
  };

  return (
    <section
      className="min-h-screen flex items-center justify-center px-4 py-16 lg:py-0"
      data-testid="hero-section"
    >
      <div className="container mx-auto max-w-7xl">
        <div className="flex flex-col lg:flex-row items-center gap-8 lg:gap-16">
          {/* Text Content - Left Side */}
          <div className="flex-1 text-center lg:text-left">
            <h1
              className="text-4xl md:text-5xl lg:text-6xl font-bold text-base-content mb-6"
              data-testid="hero-headline"
            >
              {HERO_CONTENT.headline}
            </h1>
            <p
              className="text-lg md:text-xl text-base-content/80 mb-8 max-w-2xl mx-auto lg:mx-0"
              data-testid="hero-subheadline"
            >
              {HERO_CONTENT.subheadline}
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center lg:justify-start">
              <button
                onClick={handleGetStarted}
                className="btn btn-primary btn-lg"
                data-testid="hero-cta-primary"
              >
                {HERO_CONTENT.ctaText}
              </button>
              {HERO_CONTENT.secondaryCtaText && (
                <button
                  onClick={handleLearnMore}
                  className="btn btn-outline btn-lg"
                  data-testid="hero-cta-secondary"
                >
                  {HERO_CONTENT.secondaryCtaText}
                </button>
              )}
            </div>
          </div>

          {/* Visual Element - Right Side */}
          <div
            className="flex-1 w-full max-w-lg lg:max-w-xl"
            data-testid="hero-visual"
          >
            <div className="bg-base-200 rounded-2xl p-6 md:p-8 shadow-xl">
              <div className="space-y-4">
                {/* URL Input Demo */}
                <div className="bg-base-100 rounded-lg p-4">
                  <label className="text-sm text-base-content/80 mb-2 block">Your long URL</label>
                  <div className="flex items-center gap-2 text-base-content/80 text-sm md:text-base break-all">
                    <span className="truncate">https://example.com/very/long/path/to/your/content?with=params</span>
                  </div>
                </div>

                {/* Arrow Indicator */}
                <div className="flex justify-center">
                  <svg
                    className="w-6 h-6 text-primary"
                    fill="none"
                    stroke="currentColor"
                    viewBox="0 0 24 24"
                    aria-hidden="true"
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 14l-7 7m0 0l-7-7m7 7V3" />
                  </svg>
                </div>

                {/* Shortened URL Output */}
                <div className="bg-base-100 rounded-lg p-4 border-2 border-primary">
                  <label className="text-sm text-base-content font-medium mb-2 block">Shortened link</label>
                  <div className="flex items-center justify-between">
                    <span className="text-base-content font-semibold text-lg">short.url/abc123</span>
                    <button className="btn btn-primary btn-sm">Copy</button>
                  </div>
                </div>

                {/* Analytics Preview */}
                <div className="grid grid-cols-3 gap-2 mt-4">
                  <div className="text-center p-2 bg-base-100 rounded-lg">
                    <div className="text-xl font-bold text-primary">1.2K</div>
                    <div className="text-xs text-base-content/80">Clicks</div>
                  </div>
                  <div className="text-center p-2 bg-base-100 rounded-lg">
                    <div className="text-xl font-bold text-primary">15</div>
                    <div className="text-xs text-base-content/80">Countries</div>
                  </div>
                  <div className="text-center p-2 bg-base-100 rounded-lg">
                    <div className="text-xl font-bold text-primary">89%</div>
                    <div className="text-xs text-base-content/80">Mobile</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

export default HeroSection;
