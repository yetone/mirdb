/**
 * Main Application Component
 * Owner: Scenario 17 - Routing Integration
 * Owner: Scenario 19 - Short URL Redirect Verification
 *
 * Configures React Router routes for the URL Shortening Service.
 * The homepage is accessible at the root path "/" without authentication.
 *
 * Routes:
 * - "/" - Homepage (public)
 * - "/login" - Login page (placeholder for Scenario 5)
 * - "/register" - Register page (placeholder for Scenario 5)
 * - "/r/:shortCode" - Short URL redirect handler (Scenario 19)
 */

import { BrowserRouter, Routes, Route, useParams } from 'react-router-dom'
import { useEffect, useState } from 'react'
import { api } from './api'
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

/**
 * Short URL Redirect Handler Component
 * Owner: Scenario 19 - Short URL Redirect Verification
 *
 * Handles /r/:shortCode routes by fetching the original URL from the API
 * and performing a 302-equivalent redirect via window.location.
 */
function RedirectHandler() {
  const { shortCode } = useParams<{ shortCode: string }>()
  const [error, setError] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    const performRedirect = async () => {
      if (!shortCode) {
        setError('Invalid short code')
        setIsLoading(false)
        return
      }

      try {
        const response = await api.get(`/api/urls/${shortCode}`)
        const { original_url } = response.data
        if (original_url) {
          // Increment click count before redirecting
          await api.post(`/api/urls/${shortCode}/click`).catch(() => {
            // Silently ignore click tracking errors
          })
          // Perform redirect to original URL
          window.location.href = original_url
        } else {
          setError('Short URL not found')
          setIsLoading(false)
        }
      } catch {
        setError('Short URL not found or expired')
        setIsLoading(false)
      }
    }

    performRedirect()
  }, [shortCode])

  if (isLoading) {
    return (
      <div data-testid="redirect-loading" className="min-h-screen flex items-center justify-center bg-base-100">
        <div className="text-center">
          <span className="loading loading-spinner loading-lg text-primary"></span>
          <p className="mt-4 text-base-content/70">Redirecting...</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div data-testid="redirect-error" className="min-h-screen flex items-center justify-center bg-base-100">
        <div className="text-center">
          <h1 className="text-2xl font-bold text-error">Error</h1>
          <p className="text-base-content/70">{error}</p>
        </div>
      </div>
    )
  }

  return null
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
            {/* Short URL redirect route - Scenario 19 */}
            <Route path="/r/:shortCode" element={<RedirectHandler />} />
          </Routes>
        </AuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  )
}

export default App
