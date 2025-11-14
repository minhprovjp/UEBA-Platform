// src/api/client.js
import axios from 'axios';

const baseURL = (import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000').replace(/\/$/, '');

export const apiClient = axios.create({
  baseURL,
  headers: {
    'Content-Type': 'application/json',
    'x-api-key': import.meta.env.VITE_API_KEY || '',
  },
  timeout: 30000,
});

export const cleanParams = (obj = {}) =>
  Object.fromEntries(Object.entries(obj).filter(([, v]) => v !== undefined && v !== null && v !== ''));
