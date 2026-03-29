import { Routes, Route } from 'react-router-dom';
import { Home } from './pages/Home';

function Register() {
  return (
    <div className="min-h-screen flex items-center justify-center">
      <h1 className="text-2xl font-bold">Registration Page</h1>
    </div>
  );
}

function Demo() {
  return (
    <div className="min-h-screen flex items-center justify-center">
      <h1 className="text-2xl font-bold">Demo Page</h1>
    </div>
  );
}

function App() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/register" element={<Register />} />
      <Route path="/demo" element={<Demo />} />
    </Routes>
  );
}

export default App;
