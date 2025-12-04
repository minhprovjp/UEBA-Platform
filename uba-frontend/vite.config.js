// uba_frontend/vite.config.js
import path from "path" // <-- Thêm dòng này
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react-swc'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      // Sửa dòng này từ '/src' thành:
      '@': path.resolve(__dirname, "./src"), 
    },
  },
})