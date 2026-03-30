import React from 'react'
import { Routes, Route } from 'react-router-dom'
import Home from '@/pages/Home'
import Navbar from '@/components/Navbar'
import BackgroundEffect from '@/components/BackgroundEffect'

const App: React.FC = () => {
  return (
    <div className="min-h-screen">
      <BackgroundEffect />
      <Navbar />
      <main className="pt-16">
        <Routes>
          <Route path="/" element={<Home />} />
        </Routes>
      </main>
    </div>
  )
}

export default App
