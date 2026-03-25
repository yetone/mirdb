import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { AuthProvider } from './contexts/AuthContext';
import { ThemeProvider } from './contexts/ThemeContext';
import { Home } from './pages/Home';
import './index.css';

function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <ThemeProvider>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/login" element={<div className="p-8 text-center"><h1 className="text-2xl font-bold">Login Page</h1><p>Sign in to your account</p></div>} />
            <Route path="/register" element={<div className="p-8 text-center"><h1 className="text-2xl font-bold">Register Page</h1><p>Create your account</p></div>} />
            <Route path="/dashboard" element={<div className="p-8 text-center"><h1 className="text-2xl font-bold">Dashboard</h1><p>Your dashboard</p></div>} />
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
