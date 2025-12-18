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
    e.preventDefault();
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
      toast.error(t('login.failed'));
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