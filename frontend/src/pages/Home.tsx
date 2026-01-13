import { Link } from 'react-router-dom'

function Home() {
  return (
    <div className="home-page">
      <header>
        <h1>URL Shortener</h1>
        <p>Shorten your URLs quickly and easily</p>
      </header>
      <nav>
        <Link to="/login" data-testid="login-link">
          Login
        </Link>
        <Link to="/register" data-testid="register-link">
          Sign Up
        </Link>
      </nav>
      <main>
        <section>
          <h2>Welcome to URL Shortener</h2>
          <p>Create short, memorable links in seconds.</p>
          <div className="cta-buttons">
            <Link to="/register" className="btn-primary" data-testid="get-started-btn">
              Get Started
            </Link>
            <Link to="/login" className="btn-secondary" data-testid="login-btn">
              Login
            </Link>
          </div>
        </section>
      </main>
    </div>
  )
}

export default Home
