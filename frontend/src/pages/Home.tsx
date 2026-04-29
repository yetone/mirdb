import { Helmet } from 'react-helmet-async';
import type { SEOConfig } from '../types/home';

const SEO_CONFIG: SEOConfig = {
  title: 'Shorten Links, Track Clicks | URL Shortener',
  description: 'Free URL shortener with analytics. Create short links, track clicks, and measure performance. Get started in seconds.',
  canonicalUrl: 'https://urlshortener.example.com',
  ogImage: 'https://urlshortener.example.com/og-image.png',
};

const STRUCTURED_DATA = {
  '@context': 'https://schema.org',
  '@type': 'WebApplication',
  name: 'URL Shortener',
  description: SEO_CONFIG.description,
  url: SEO_CONFIG.canonicalUrl,
  applicationCategory: 'UtilityApplication',
  operatingSystem: 'Web',
  offers: {
    '@type': 'Offer',
    price: '0',
    priceCurrency: 'USD',
  },
};

function Home() {
  return (
    <>
      <Helmet>
        <title>{SEO_CONFIG.title}</title>
        <meta name="description" content={SEO_CONFIG.description} />
        <link rel="canonical" href={SEO_CONFIG.canonicalUrl} />

        {/* Open Graph tags */}
        <meta property="og:title" content={SEO_CONFIG.title} />
        <meta property="og:description" content={SEO_CONFIG.description} />
        <meta property="og:image" content={SEO_CONFIG.ogImage} />
        <meta property="og:url" content={SEO_CONFIG.canonicalUrl} />
        <meta property="og:type" content="website" />

        {/* Twitter Card tags */}
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content={SEO_CONFIG.title} />
        <meta name="twitter:description" content={SEO_CONFIG.description} />
        <meta name="twitter:image" content={SEO_CONFIG.ogImage} />

        {/* Structured data */}
        <script type="application/ld+json">
          {JSON.stringify(STRUCTURED_DATA)}
        </script>
      </Helmet>

      <div className="min-h-screen flex flex-col">
        <header role="banner">
          <nav role="navigation" aria-label="Main navigation" className="navbar bg-base-100 shadow-lg">
            <div className="flex-1">
              <a href="/" className="btn btn-ghost text-xl">URL Shortener</a>
            </div>
            <div className="flex-none">
              <ul className="menu menu-horizontal px-1">
                <li><a href="/login">Login</a></li>
                <li><a href="/register">Register</a></li>
              </ul>
            </div>
          </nav>
        </header>

        <main id="main-content" role="main" className="flex-grow">
          <section aria-labelledby="hero-heading" className="hero min-h-[60vh] bg-gradient-to-br from-primary to-secondary">
            <div className="hero-content text-center">
              <div className="max-w-2xl">
                <h1 id="hero-heading" className="text-5xl font-bold text-white">
                  Shorten Links, Track Clicks
                </h1>
                <p className="py-6 text-lg text-white/90">
                  Create short, memorable links and track their performance with powerful analytics.
                </p>
                <div className="flex gap-4 justify-center">
                  <a href="/register" className="btn btn-primary btn-lg">Get Started Free</a>
                  <a href="#features" className="btn btn-outline btn-lg text-white border-white hover:bg-white hover:text-primary">
                    Learn More
                  </a>
                </div>
              </div>
            </div>
          </section>

          <section id="features" aria-labelledby="features-heading" className="py-20 px-4">
            <div className="max-w-6xl mx-auto">
              <h2 id="features-heading" className="text-3xl font-bold text-center mb-12">
                Why Choose Us?
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                <article className="card bg-base-200 shadow-xl">
                  <div className="card-body">
                    <h3 className="card-title">URL Shortening</h3>
                    <p>Create short, shareable links instantly.</p>
                  </div>
                </article>
                <article className="card bg-base-200 shadow-xl">
                  <div className="card-body">
                    <h3 className="card-title">Analytics</h3>
                    <p>Track clicks, locations, and devices.</p>
                  </div>
                </article>
                <article className="card bg-base-200 shadow-xl">
                  <div className="card-body">
                    <h3 className="card-title">Dashboard</h3>
                    <p>Manage all your links in one place.</p>
                  </div>
                </article>
                <article className="card bg-base-200 shadow-xl">
                  <div className="card-body">
                    <h3 className="card-title">Security</h3>
                    <p>Enterprise-grade security for your links.</p>
                  </div>
                </article>
              </div>
            </div>
          </section>

          <section aria-labelledby="how-it-works-heading" className="py-20 px-4 bg-base-200">
            <div className="max-w-4xl mx-auto text-center">
              <h2 id="how-it-works-heading" className="text-3xl font-bold mb-12">
                How It Works
              </h2>
              <div className="flex flex-col md:flex-row gap-8 justify-center">
                <div className="flex-1">
                  <div className="text-4xl mb-4">1</div>
                  <h3 className="font-bold text-xl mb-2">Paste</h3>
                  <p>Enter your long URL</p>
                </div>
                <div className="flex-1">
                  <div className="text-4xl mb-4">2</div>
                  <h3 className="font-bold text-xl mb-2">Shorten</h3>
                  <p>Get a short link instantly</p>
                </div>
                <div className="flex-1">
                  <div className="text-4xl mb-4">3</div>
                  <h3 className="font-bold text-xl mb-2">Share</h3>
                  <p>Share and track clicks</p>
                </div>
              </div>
            </div>
          </section>
        </main>

        <footer role="contentinfo" className="footer p-10 bg-neutral text-neutral-content">
          <nav aria-label="Footer navigation">
            <h4 className="footer-title">Product</h4>
            <a href="/features" className="link link-hover">Features</a>
            <a href="/pricing" className="link link-hover">Pricing</a>
            <a href="/api" className="link link-hover">API</a>
          </nav>
          <nav aria-label="Legal">
            <h4 className="footer-title">Legal</h4>
            <a href="/terms" className="link link-hover">Terms of Service</a>
            <a href="/privacy" className="link link-hover">Privacy Policy</a>
          </nav>
          <nav aria-label="Contact">
            <h4 className="footer-title">Contact</h4>
            <a href="mailto:support@urlshortener.example.com" className="link link-hover">Support</a>
          </nav>
          <div>
            <p>&copy; 2024 URL Shortener. All rights reserved.</p>
          </div>
        </footer>
      </div>
    </>
  );
}

export default Home;

export { SEO_CONFIG, STRUCTURED_DATA };
