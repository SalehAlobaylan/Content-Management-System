package controllers

import (
	"strings"

	"gorm.io/gorm"
)

type deliveryLanguage string

const (
	deliveryLanguageArabic  deliveryLanguage = "ar"
	deliveryLanguageEnglish deliveryLanguage = "en"
	deliveryLanguageBoth    deliveryLanguage = "both"
)

func parseDeliveryLanguage(raw string) (deliveryLanguage, bool) {
	switch deliveryLanguage(strings.ToLower(strings.TrimSpace(raw))) {
	case "", deliveryLanguageBoth:
		return deliveryLanguageBoth, true
	case deliveryLanguageArabic:
		return deliveryLanguageArabic, true
	case deliveryLanguageEnglish:
		return deliveryLanguageEnglish, true
	default:
		return "", false
	}
}

// applyDeliveryLanguage keeps legacy unknown-language rows neutral during the
// explicit ingest migration. Once a source declares ar/en, the feed obeys the
// requested delivery mode entirely on the server.
func applyDeliveryLanguage(query *gorm.DB, language deliveryLanguage) *gorm.DB {
	if language == deliveryLanguageBoth {
		return query
	}
	return query.Where("(content_language = ? OR content_language IS NULL)", string(language))
}
