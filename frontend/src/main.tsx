import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { AuthProvider } from './contexts/AuthContext';
import { ThemeProvider } from './contexts/ThemeContext';
import { Home } from './pages/Home';

function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <ThemeProvider>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/register" element={<div className="min-h-screen bg-base-200 flex items-center justify-center"><h1 className="text-3xl">Register Page</h1></div>} />
            <Route path="/login" element={<div className="min-h-screen bg-base-200 flex items-center justify-center"><h1 className="text-3xl">Login Page</h1></div>} />
            <Route path="/dashboard" element={<div className="min-h-screen bg-base-200 flex items-center justify-center"><h1 className="text-3xl">Dashboard</h1></div>} />
          </Routes>
        </ThemeProvider>
      </AuthProvider>
    </BrowserRouter>
  );
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
