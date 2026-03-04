import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Home from './pages/Home'

function App() {
  return (
    <BrowserRouter>
      <div data-theme="light" className="min-h-screen">
        <Routes>
          <Route path="/" element={<Home />} />
          {/* Placeholder routes for other pages */}
          <Route path="/register" element={<div>Register Page</div>} />
          <Route path="/login" element={<div>Login Page</div>} />
        </Routes>
      </div>
    </BrowserRouter>
  )
}

export default App
