import React from 'react';
import { useTranslation } from 'react-i18next';
import { Button } from "@/components/ui/button";

export default function LanguageSwitcher() {
  const { i18n } = useTranslation();

  const toggleLanguage = () => {
    const newLang = i18n.language === 'vi' ? 'en' : 'vi';
    i18n.changeLanguage(newLang);
  };

  return (
    <Button 
      variant="outline" 
      size="sm" 
      onClick={toggleLanguage}
      className="border-zinc-700 bg-zinc-900 text-zinc-300 hover:text-white hover:bg-zinc-800"
    >
      {i18n.language === 'vi' ? '🇻🇳 VN' : '🇺🇸 EN'}
    </Button>
  );
}