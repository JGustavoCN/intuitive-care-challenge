import axios from "axios";

const baseURL = import.meta.env.VITE_API_URL || "/api";

const api = axios.create({
  baseURL: baseURL,
  timeout: 10000,
});

export default api;
