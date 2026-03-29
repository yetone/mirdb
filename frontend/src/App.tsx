import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { Home } from './pages/Home';

// Placeholder for registration page
function RegisterPage() {
  return (
    <div className="min-h-screen flex items-center justify-center" data-testid="register-page">
      <h1 className="text-2xl font-bold">Registration Page</h1>
    </div>
  );
}

// Placeholder for demo page
function DemoPage() {
  return (
    <div className="min-h-screen flex items-center justify-center" data-testid="demo-page">
      <h1 className="text-2xl font-bold">Demo Page</h1>
    </div>
  );
}

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/register" element={<RegisterPage />} />
        <Route path="/demo" element={<DemoPage />} />
      </Routes>
    </Router>
  );
}

export default App;
