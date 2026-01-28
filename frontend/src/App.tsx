import { Routes, Route } from 'react-router-dom';
import { HeroSection } from './components/home';

function App() {
  return (
    <div data-theme="dark" className="min-h-screen">
      <Routes>
        <Route path="/" element={<HeroSection />} />
        <Route path="/register" element={<div>Register Page</div>} />
        <Route path="/login" element={<div>Login Page</div>} />
      </Routes>
    </div>
  );
}

export default App;
