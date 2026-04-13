/**
 * MirDB Homepage Application
 */
import { Features } from './components/Features';
import { PRODUCT_NAME, TAGLINE, GITHUB_URL } from './utils/constants';
import './styles/globals.css';
import './styles/responsive.css';

function App() {
  return (
    <>
      <a href="#main" className="skip-link">Skip to main content</a>

      <main id="main">
        {/* Hero Section - placeholder for Scenario 1 */}
        <section id="hero" className="section" style={{ textAlign: 'center', padding: '4rem 1rem' }}>
          <div className="container">
            <h1>{PRODUCT_NAME}</h1>
            <p style={{ fontSize: '1.25rem', color: 'var(--color-text-light)' }}>{TAGLINE}</p>
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
          </div>
        </section>

        {/* Features Section - Scenario 2 */}
        <Features />
      </main>
    </>
  );
}

export default App;
