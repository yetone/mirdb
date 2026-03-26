import { Routes, Route } from 'react-router-dom'
import Home from './pages/Home'
import { Navbar } from './components/Navbar'

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
      <Navbar />
      <div className="pt-16"> {/* Padding to account for fixed navbar */}
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
