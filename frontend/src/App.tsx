import { Routes, Route } from 'react-router-dom'
import Home from './pages/Home'
import './App.css'

function App() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/login" element={<div>Login Page</div>} />
      <Route path="/register" element={<div>Register Page</div>} />
    </Routes>
  )
}

export default App
