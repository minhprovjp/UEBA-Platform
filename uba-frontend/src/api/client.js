// src/api/client.js
import axios from 'axios';

const API_URL = "http://localhost:8000"; // API backend của bạn
const API_KEY = "default_secret_key_change_me"; // API Key của bạn

export const apiClient = axios.create({
  baseURL: API_URL,
  headers: { 'X-API-Key': API_KEY }
});