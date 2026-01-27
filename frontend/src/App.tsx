import { useState } from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { Navbar } from './components/Navbar';
import { BackgroundEffect } from './components/BackgroundEffect';
import Home from './pages/Home';
import './index.css';

function App() {
  const [theme, setTheme] = useState<'light' | 'dark'>('dark');

  const toggleTheme = () => {
    setTheme((prev) => (prev === 'dark' ? 'light' : 'dark'));
  };

  return (
    <div data-theme={theme} className="min-h-screen bg-base-100 text-base-content">
      <BrowserRouter>
        <BackgroundEffect />
        <Navbar theme={theme} onThemeToggle={toggleTheme} />
        <main className="pt-16">
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/login" element={<div className="p-8 text-center">Login Page</div>} />
            <Route path="/register" element={<div className="p-8 text-center">Register Page</div>} />
            <Route path="/dashboard" element={<div className="p-8 text-center">Dashboard</div>} />
          </Routes>
        </main>
      </BrowserRouter>
    </div>
  );
}

export default App;
