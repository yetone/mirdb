import { Routes, Route } from 'react-router-dom'
import Home from './pages/Home'
import Login from './pages/Login'

function App() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/login" element={<Login />} />
    </Routes>
  )
}

export default App
