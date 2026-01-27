import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import { ThemeProvider } from './contexts/ThemeContext'
import { Home } from './pages/Home'
import './index.css'

/**
 * Mock Login page for routing
 */
function LoginPage() {
  return (
    <div className="min-h-screen bg-base-100 flex items-center justify-center" data-testid="login-page">
      <div className="card bg-base-200 p-8">
        <h1 className="text-2xl font-bold mb-4">Login</h1>
        <p>Please sign in to continue</p>
      </div>
    </div>
  )
}

/**
 * Mock Register page for routing
 */
function RegisterPage() {
  return (
    <div className="min-h-screen bg-base-100 flex items-center justify-center" data-testid="register-page">
      <div className="card bg-base-200 p-8">
        <h1 className="text-2xl font-bold mb-4">Register</h1>
        <p>Create your account</p>
      </div>
    </div>
  )
}

function App() {
  return (
    <ThemeProvider>
      <Router>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/login" element={<LoginPage />} />
          <Route path="/register" element={<RegisterPage />} />
        </Routes>
      </Router>
    </ThemeProvider>
  )
}

export default App
