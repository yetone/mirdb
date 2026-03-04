import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { Home } from './pages/Home'
import './index.css'

function App() {
  return (
    <BrowserRouter>
      <div data-theme="dark" className="min-h-screen">
        <Routes>
          <Route path="/" element={<Home />} />
          {/* Future routes will be added:
            * /login - Login page
            * /register - Registration page
            * /dashboard - User dashboard
            */}
        </Routes>
      </div>
    </BrowserRouter>
  )
}

export default App
