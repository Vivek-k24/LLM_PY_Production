import React, { useState } from "react";
import "./App.css";
import axios from "axios";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";

function Home() {
  const [file, setFile] = useState(null);

  const handleFileChange = (event) => {
    setFile(event.target.files[0]);
  };

  const handleFileUpload = async () => {
    if (!file) {
      alert("Please select a file first!");
      return;
    }

    const formData = new FormData();
    formData.append("file", file);

    try {
      const backendUrl = process.env.REACT_APP_BACKEND_URL || "http://localhost:8000";
      const response = await axios.post(`${backendUrl}/upload/`, formData);
      alert(response.data.message);
    } catch (error) {
      console.error("Error uploading the file:", error);
      alert("Failed to upload file.");
    }
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>Upload Your File</h1>
        <input type="file" onChange={handleFileChange} />
        <button onClick={handleFileUpload}>Upload</button>
      </header>
    </div>
  );
}

function NotFound() {
  return (
    <div className="App">
      <header className="App-header">
        <h1>404 - Page Not Found</h1>
      </header>
    </div>
  );
}

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
    </Router>
  );
}

export default App;
