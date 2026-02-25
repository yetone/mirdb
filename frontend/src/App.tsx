/**
 * Main Application Component
 * Owner: Scenario 17 - Routing Integration
 *
 * Configures React Router routes for the URL Shortening Service.
 * The homepage is accessible at the root path "/" without authentication.
 *
 * Routes:
 * - "/" - Homepage (public)
 * - "/login" - Login page (placeholder for Scenario 5)
 * - "/register" - Register page (placeholder for Scenario 5)
 */

import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { ThemeProvider } from './contexts/ThemeContext'
import { AuthProvider } from './contexts/AuthContext'
import Home from './pages/Home'

/**
 * Login page placeholder component
 * Actual implementation owned by Scenario 5
 */
function LoginPage() {
  return (
    <div data-testid="login-page" className="min-h-screen flex items-center justify-center bg-base-100">
      <div className="text-center">
        <h1 className="text-2xl font-bold text-base-content">Login</h1>
        <p className="text-base-content/70">Login page - coming soon</p>
      </div>
    </div>
  )
}

/**
 * Register page placeholder component
 * Actual implementation owned by Scenario 5
 */
function RegisterPage() {
  return (
    <div data-testid="register-page" className="min-h-screen flex items-center justify-center bg-base-100">
      <div className="text-center">
        <h1 className="text-2xl font-bold text-base-content">Register</h1>
        <p className="text-base-content/70">Registration page - coming soon</p>
      </div>
    </div>
  )
}

function App() {
  return (
    <BrowserRouter>
      <ThemeProvider>
        <AuthProvider>
          <Routes>
            {/* Homepage at root path - Scenario 17 */}
            <Route path="/" element={<Home />} />
            {/* Login/Register routes for navigation testing - Scenario 5, 17 */}
            <Route path="/login" element={<LoginPage />} />
            <Route path="/register" element={<RegisterPage />} />
          </Routes>
        </AuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  )
}

export default App
