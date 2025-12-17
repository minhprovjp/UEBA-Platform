// src/i18n.js
import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';
import LanguageDetector from 'i18next-browser-languagedetector';

// Import các file ngôn ngữ (chúng ta sẽ tạo ở bước sau)
import translationEN from './locales/en/translation_en.json';
import translationVI from './locales/vi/translation_vi.json';

const resources = {
  en: {
    translation: translationEN
  },
  vi: {
    translation: translationVI
  }
};

i18n
  .use(LanguageDetector) // Tự động phát hiện ngôn ngữ trình duyệt
  .use(initReactI18next) // Kết nối với React
  .init({
    resources,
    fallbackLng: 'en', // Ngôn ngữ mặc định nếu không phát hiện được
    debug: true,       // Bật log để debug lúc phát triển
    
    interpolation: {
      escapeValue: false // React đã tự xử lý XSS nên không cần escape
    }
  });

export default i18n;