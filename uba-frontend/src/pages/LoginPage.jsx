import React, { useState } from 'react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ShieldAlert } from 'lucide-react';
import { apiClient } from '@/api/client';
import { toast } from 'sonner';
import { useTranslation } from 'react-i18next';

export default function LoginPage({ onLoginSuccess }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const { t } = useTranslation();

  const handleLogin = async (e) => {
    if (e && e.preventDefault) e.preventDefault();
    setLoading(true);
    
    try {
      const formData = new FormData();
      formData.append('username', username);
      formData.append('password', password);

      const res = await apiClient.post('/api/login', formData, {
        headers: { 'Content-Type': 'multipart/form-data' } 
      });

      const token = res.data.access_token;
      localStorage.setItem('uba_token', token);
      
      toast.success(t('login.success'));
      onLoginSuccess();
      
    } catch (error) {
      console.error("Full Login Error Object:", error);

      let errorMessage = "";

      // -----------------------------------------------------------
      // CASE 1: SERVER RESPONDED (Error 4xx, 5xx)
      // -----------------------------------------------------------
      if (error.response) {
        // Priority 1: Get specific message from Backend (e.g., "Incorrect username or password")
        const serverMessage = error.response.data?.detail;

        if (serverMessage) {
            if (typeof serverMessage === 'object' && Array.isArray(serverMessage)) {
                // Handle validation errors (array of objects)
                errorMessage = serverMessage.map(err => err.msg).join(', ');
            } else {
                errorMessage = serverMessage; 
            }
        } 
        // Priority 2: Guess based on status code
        else if (error.response.status === 401) {
            errorMessage = "Invalid credentials (401).";
        } else if (error.response.status === 500) {
            errorMessage = "Internal Server Error (500). Please check backend logs.";
        } else if (error.response.status === 404) {
            errorMessage = "Login API endpoint not found (404).";
        } else {
            errorMessage = `Server Error: ${error.response.status} ${error.response.statusText}`;
        }
      } 
      
      // -----------------------------------------------------------
      // CASE 2: NO RESPONSE (Server down, Network error)
      // -----------------------------------------------------------
      else if (error.request) {
        if (error.code === 'ERR_NETWORK') {
            errorMessage = "Cannot connect to Server!";
        } else if (error.code === 'ECONNABORTED') {
            errorMessage = "Connection timed out. Server took too long to respond.";
        } else {
            errorMessage = "Network Error. No response received from server.";
        }
      } 
      
      // -----------------------------------------------------------
      // CASE 3: REQUEST SETUP ERROR
      // -----------------------------------------------------------
      else {
        errorMessage = `Client Error: ${error.message}`;
      }

      // Display the final English error message
      toast.error(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="h-screen flex items-center justify-center bg-black text-white">
      <div className="w-full max-w-md p-8 space-y-6 bg-zinc-900 border border-zinc-800 rounded-xl shadow-2xl">
        <div className="flex flex-col items-center justify-center text-center space-y-2">
          <div className="p-3 bg-zinc-800 rounded-full">
            <ShieldAlert className="w-10 h-10 text-primary-500" />
          </div>
          <h1 className="text-2xl font-bold tracking-tight">{t('login.title')}</h1>
          <p className="text-sm text-zinc-400">{t('login.subtitle')}</p>
        </div>

        <form onSubmit={handleLogin} className="space-y-4">
          <div className="space-y-2">
            <label className="text-sm font-medium leading-none">{t('login.username')}</label>
            <Input 
              value={username} onChange={e => setUsername(e.target.value)}
              className="bg-zinc-950 border-zinc-800 focus:border-primary-500" 
              placeholder={t('login.username')} required
            />
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium leading-none">{t('login.password')}</label>
            <Input 
              type="password"
              value={password} onChange={e => setPassword(e.target.value)}
              className="bg-zinc-950 border-zinc-800 focus:border-primary-500" 
              placeholder={t('login.password')} required
            />
          </div>
          <Button type="submit" className="w-full bg-primary-600 hover:bg-primary-700" disabled={loading}>
            {loading ? t('login.verifying') : t('login.signin')}
          </Button>
        </form>
      </div>
    </div>
  );
}