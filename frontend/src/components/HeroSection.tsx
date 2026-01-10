import { Link } from 'react-router-dom'

function HeroSection() {
  return (
    <section className="hero min-h-screen bg-gradient-to-br from-primary to-secondary">
      <div className="hero-content text-center">
        <div className="max-w-lg">
          <h1 className="text-5xl font-bold text-primary-content">
            Shorten, Share, Track
          </h1>
          <p className="py-6 text-primary-content/80">
            Transform long URLs into memorable short links. Track clicks, analyze performance, and manage all your links in one place.
          </p>
          <div className="flex flex-col items-center gap-4">
            <Link to="/register" className="btn btn-accent btn-lg">
              Get Started Free
            </Link>
            <p className="text-primary-content/70">
              Already have an account?{' '}
              <Link
                to="/login"
                className="link link-hover text-primary-content font-semibold underline"
                data-testid="login-link"
              >
                Log in
              </Link>
            </p>
          </div>
        </div>
      </div>
    </section>
  )
}

export default HeroSection
