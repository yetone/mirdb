import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { Home } from './pages/Home'
import { ThemeProvider } from './contexts/ThemeContext'
import './index.css'

/**
 * Placeholder Login Page
 * Minimal implementation for navigation testing
 */
function Login() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-base-100" data-testid="login-page">
      <h1 className="text-3xl font-bold">Login Page</h1>
    </div>
  )
}

/**
 * Placeholder Register Page
 * Minimal implementation for navigation testing
 */
function Register() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-base-100" data-testid="register-page">
      <h1 className="text-3xl font-bold">Register Page</h1>
    </div>
  )
}

function App() {
  return (
    <ThemeProvider defaultTheme="dark">
      <BrowserRouter>
        <div data-theme="dark" className="min-h-screen transition-colors duration-300">
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/login" element={<Login />} />
            <Route path="/register" element={<Register />} />
            {/* Future routes will be added:
              * /dashboard - User dashboard
              */}
          </Routes>
        </div>
      </BrowserRouter>
    </ThemeProvider>
  )
}

export default App
