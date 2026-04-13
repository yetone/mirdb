/**
 * MirDB Homepage Application
 */
import { Features } from './components/Features';
import { QuickStart } from './components/QuickStart/QuickStart';
import { StatusBadge } from './components/Badges';
import { Demo } from './components/Demo';
import { Roadmap } from './components/Roadmap';
import { Footer } from './components/Footer';
import { PRODUCT_NAME, TAGLINE, GITHUB_URL, CIRCLECI_URL, CIRCLECI_BADGE_URL } from './utils/constants';
import './styles/globals.css';
import './styles/responsive.css';

function App() {
  return (
    <>
      <a href="#main-content" className="skip-link">
        Skip to main content
      </a>
      <main id="main-content">
        {/* Hero Section - placeholder for Scenario 1 */}
        <section id="hero" className="section" style={{ textAlign: 'center', padding: '4rem 1rem' }}>
          <div className="container">
            <h1>{PRODUCT_NAME}</h1>
            <p style={{ fontSize: '1.25rem', color: 'var(--color-text-muted)' }}>{TAGLINE}</p>
            <a
              href={GITHUB_URL}
              target="_blank"
              rel="noopener noreferrer"
              style={{
                display: 'inline-block',
                marginTop: '1.5rem',
                padding: '0.75rem 1.5rem',
                backgroundColor: 'var(--color-primary)',
                color: 'white',
                borderRadius: '0.5rem',
                fontWeight: 600,
              }}
            >
              View on GitHub
            </a>
            <div className="status-badges" style={{ marginTop: '1rem' }}>
              <StatusBadge
                src={CIRCLECI_BADGE_URL}
                alt="Build Status"
                href={CIRCLECI_URL}
              />
            </div>
          </div>
        </section>

        {/* Features Section - Scenario 2 */}
        <Features />

        {/* Quick Start Section - Scenario 3 */}
        <QuickStart />

        {/* Demo Section - Scenario 6 */}
        <Demo />

        {/* Roadmap Section - Scenario 7 */}
        <Roadmap />
      </main>

      {/* Footer Section - Scenario 17 */}
      <Footer />
    </>
  );
}

export default App;
