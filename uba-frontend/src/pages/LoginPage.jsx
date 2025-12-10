import React, { useState } from 'react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ShieldAlert } from 'lucide-react';
import { apiClient } from '@/api/client';
import { toast } from 'sonner';

export default function LoginPage({ onLoginSuccess }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);

  const handleLogin = async (e) => {
    e.preventDefault();
    setLoading(true);
    
    try {
      // Form Data chuẩn OAuth2
      const formData = new FormData();
      formData.append('username', username);
      formData.append('password', password);

      const res = await apiClient.post('/api/login', formData, {
        headers: { 'Content-Type': 'multipart/form-data' } // Quan trọng
      });

      const token = res.data.access_token;
      
      // Lưu token vào localStorage
      localStorage.setItem('uba_token', token);
      
      toast.success("Đăng nhập thành công!");
      onLoginSuccess(); // Báo cho App biết để chuyển trang
      
    } catch (error) {
      toast.error("Đăng nhập thất bại: Sai tài khoản hoặc mật khẩu");
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
          <h1 className="text-2xl font-bold tracking-tight">Security Command Center</h1>
          <p className="text-sm text-zinc-400">UEBA Platform Access Control</p>
        </div>

        <form onSubmit={handleLogin} className="space-y-4">
          <div className="space-y-2">
            <label className="text-sm font-medium leading-none">Username</label>
            <Input 
              value={username} onChange={e => setUsername(e.target.value)}
              className="bg-zinc-950 border-zinc-800 focus:border-primary-500" 
              placeholder="admin" required
            />
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium leading-none">Password</label>
            <Input 
              type="password"
              value={password} onChange={e => setPassword(e.target.value)}
              className="bg-zinc-950 border-zinc-800 focus:border-primary-500" 
              required
            />
          </div>
          <Button type="submit" className="w-full bg-primary-600 hover:bg-primary-700" disabled={loading}>
            {loading ? "Verifying..." : "Sign In"}
          </Button>
        </form>
      </div>
    </div>
  );
}