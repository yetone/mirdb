import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import { FeaturesSection } from './components/home/FeaturesSection'

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<FeaturesSection />} />
      </Routes>
    </Router>
  )
}

export default App
