import { Routes, Route } from 'react-router-dom'
import Home from './pages/Home'
import { Navbar } from './components/Navbar'

/**
 * Skip to Main Content Link Component
 * Hidden until focused - provides keyboard users quick navigation to main content
 * Requirements: NFR-3 (WCAG 2.1 AA compliance)
 */
function SkipLink() {
  const handleClick = (e: React.MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault()
    const mainContent = document.getElementById('main-content')
    if (mainContent) {
      mainContent.focus()
      mainContent.scrollIntoView({ behavior: 'smooth' })
    }
  }

  return (
    <a
      href="#main-content"
      data-testid="skip-link"
      onClick={handleClick}
      className="absolute left-0 z-[100] px-4 py-2 bg-primary text-primary-content rounded-md shadow-lg outline-none ring-2 ring-primary-focus transform -translate-y-full focus:translate-y-4 transition-transform"
      style={{ top: 0, left: '1rem' }}
    >
      Skip to main content
    </a>
  )
}

// Placeholder Login Page for navigation testing
function LoginPage() {
  return (
    <div data-testid="login-page" className="min-h-screen flex items-center justify-center">
      <h1 className="text-3xl font-bold">Login</h1>
    </div>
  )
}

// Placeholder Register Page for navigation testing
function RegisterPage() {
  return (
    <div data-testid="register-page" className="min-h-screen flex items-center justify-center">
      <h1 className="text-3xl font-bold">Register</h1>
    </div>
  )
}

function App() {
  return (
    <>
      <SkipLink />
      <Navbar />
      <div id="main-content" tabIndex={-1} className="pt-16 outline-none"> {/* Padding to account for fixed navbar */}
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/login" element={<LoginPage />} />
          <Route path="/register" element={<RegisterPage />} />
        </Routes>
      </div>
    </>
  )
}

export default App
