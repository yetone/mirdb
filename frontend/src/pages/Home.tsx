import HowItWorksSection from '../components/HowItWorksSection'

const Home: React.FC = () => {
  const scrollToSection = (sectionId: string) => {
    const element = document.getElementById(sectionId)
    if (element) {
      element.scrollIntoView({ behavior: 'smooth' })
    }
  }

  return (
    <div className="min-h-screen bg-base-100">
      {/* Navigation Header */}
      <nav className="navbar bg-base-100 shadow-lg sticky top-0 z-50">
        <div className="flex-1">
          <a href="/" className="btn btn-ghost text-xl font-bold">
            URL Shortener
          </a>
        </div>
        <div className="flex-none gap-2">
          <button
            className="btn btn-ghost"
            onClick={() => scrollToSection('features')}
          >
            Features
          </button>
          <button
            className="btn btn-ghost"
            onClick={() => scrollToSection('how-it-works')}
            data-testid="nav-how-it-works"
          >
            How It Works
          </button>
          <a href="/login" className="btn btn-ghost">
            Log In
          </a>
          <a href="/register" className="btn btn-primary">
            Sign Up
          </a>
        </div>
      </nav>

      {/* Hero Section */}
      <section className="hero min-h-[70vh] bg-base-200">
        <div className="hero-content text-center">
          <div className="max-w-2xl">
            <h1 className="text-5xl font-bold">Shorten. Share. Track.</h1>
            <p className="py-6 text-xl">
              Create short URLs and track every click with powerful analytics.
              Start shortening your links today.
            </p>
            <div className="flex gap-4 justify-center">
              <a href="/register" className="btn btn-primary btn-lg">
                Get Started Free
              </a>
              <a href="/login" className="btn btn-outline btn-lg">
                Log In
              </a>
            </div>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section id="features" className="py-16 px-4">
        <div className="max-w-6xl mx-auto">
          <h2 className="text-3xl md:text-4xl font-bold text-center mb-12">
            Features
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
            <div className="card bg-base-200 shadow-xl">
              <div className="card-body items-center text-center">
                <div className="text-primary mb-4">
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-12 w-12"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M13 10V3L4 14h7v7l9-11h-7z"
                    />
                  </svg>
                </div>
                <h3 className="card-title">Quick URL Shortening</h3>
                <p>Shorten any URL instantly with just one click.</p>
              </div>
            </div>
            <div className="card bg-base-200 shadow-xl">
              <div className="card-body items-center text-center">
                <div className="text-primary mb-4">
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-12 w-12"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
                    />
                  </svg>
                </div>
                <h3 className="card-title">Detailed Analytics</h3>
                <p>Track clicks, locations, and referrers in real-time.</p>
              </div>
            </div>
            <div className="card bg-base-200 shadow-xl">
              <div className="card-body items-center text-center">
                <div className="text-primary mb-4">
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-12 w-12"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"
                    />
                  </svg>
                </div>
                <h3 className="card-title">Easy Link Management</h3>
                <p>Organize, edit, and manage all your shortened links.</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* How It Works Section */}
      <HowItWorksSection />

      {/* Footer */}
      <footer className="footer footer-center p-10 bg-base-200 text-base-content">
        <div>
          <p className="font-bold">URL Shortener</p>
          <p>Shorten. Share. Track.</p>
        </div>
        <div>
          <div className="grid grid-flow-col gap-4">
            <a href="/about" className="link link-hover">
              About
            </a>
            <a href="/privacy" className="link link-hover">
              Privacy Policy
            </a>
            <a href="/terms" className="link link-hover">
              Terms of Service
            </a>
          </div>
        </div>
        <div>
          <p>Copyright © 2024 - All rights reserved</p>
        </div>
      </footer>
    </div>
  )
}

export default Home
