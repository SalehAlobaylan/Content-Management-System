package operator

import (
	"encoding/json"
	"fmt"
	"strings"
)

func storageAnswerBlocks(packet DecisionPacket, locale string) []ResponseBlock {
	for _, fact := range packet.Facts {
		if fact.Key != "storage_quality.usage" {
			continue
		}
		raw, err := json.Marshal(fact.Value)
		if err != nil {
			continue
		}
		var usage StorageSnapshot
		if json.Unmarshal(raw, &usage) != nil {
			continue
		}
		text := fmt.Sprintf("CMS tracks %s across %d non-archived content items for this tenant.", storageBytes(usage.TrackedBytes), usage.TrackedItems)
		if locale == "ar" {
			text = fmt.Sprintf("يسجّل CMS لهذا المستأجر %s عبر %d عنصر محتوى غير مؤرشف.", storageBytes(usage.TrackedBytes), usage.TrackedItems)
		}
		blocks := []ResponseBlock{{Kind: "fact", Text: text, EvidenceIDs: fact.EvidenceIDs}}
		if usage.ProviderUsedBytes != nil && usage.ProviderObjectCount != nil {
			text = fmt.Sprintf("The shared storage bucket reports %s used across %d objects (platform-wide).", storageBytes(*usage.ProviderUsedBytes), *usage.ProviderObjectCount)
			if locale == "ar" {
				text = fmt.Sprintf("تبلغ المساحة المستخدمة في حاوية التخزين المشتركة %s عبر %d كائن على مستوى المنصة.", storageBytes(*usage.ProviderUsedBytes), *usage.ProviderObjectCount)
			}
			blocks = append(blocks, ResponseBlock{Kind: "fact", Text: text, EvidenceIDs: fact.EvidenceIDs})
		}
		if usage.PolicyLimitBytes != nil {
			scope := "this tenant"
			if usage.PolicyScope == "platform_global" {
				scope = "platform default"
			}
			text = fmt.Sprintf("Configured storage limit: %s (%s).", storageBytes(*usage.PolicyLimitBytes), scope)
			if locale == "ar" {
				scope = "لهذا المستأجر"
				if usage.PolicyScope == "platform_global" {
					scope = "الإعداد الافتراضي للمنصة"
				}
				text = fmt.Sprintf("حد التخزين المضبوط: %s (%s).", storageBytes(*usage.PolicyLimitBytes), scope)
			}
			blocks = append(blocks, ResponseBlock{Kind: "fact", Text: text, EvidenceIDs: fact.EvidenceIDs})
		}
		if usage.CleanupEnabled != nil {
			state := "disabled"
			if *usage.CleanupEnabled {
				state = "enabled"
			}
			text = "Storage cleanup policy: " + state + "."
			if locale == "ar" {
				state = "معطّلة"
				if *usage.CleanupEnabled {
					state = "مفعّلة"
				}
				text = "سياسة تنظيف التخزين: " + state + "."
			}
			blocks = append(blocks, ResponseBlock{Kind: "fact", Text: text, EvidenceIDs: fact.EvidenceIDs})
		}
		return blocks
	}
	return nil
}

func storageBytes(value int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	amount, index := float64(value), 0
	for amount >= 1024 && index < len(units)-1 {
		amount /= 1024
		index++
	}
	if index == 0 {
		return fmt.Sprintf("%d B", value)
	}
	return fmt.Sprintf("%.2f %s", amount, units[index])
}

func storageUnknown(unknown, locale string) string {
	switch {
	case strings.HasPrefix(unknown, "No current policy record") || strings.HasPrefix(unknown, "No stored storage limit"):
		if locale == "ar" {
			return "لا توجد سياسة تخزين محفوظة لهذا المستأجر أو كإعداد افتراضي للمنصة."
		}
		return "No storage policy has been saved for this tenant or as a platform default."
	case strings.HasPrefix(unknown, "No current sweep record"):
		if locale == "ar" {
			return "لم تُسجّل عملية تنظيف تخزين لهذا المستأجر."
		}
		return "No storage sweep has been recorded for this tenant."
	case strings.HasPrefix(unknown, "No current quality_profile record"):
		if locale == "ar" {
			return "لا يتوفر ملف جودة افتراضي."
		}
		return "No default quality profile is available."
	case strings.HasPrefix(unknown, "The storage owner could not report"):
		if locale == "ar" {
			return "تعذّر قراءة استهلاك حاوية التخزين المشتركة؛ تظل أرقام CMS الخاصة بالمستأجر متاحة."
		}
	case strings.HasPrefix(unknown, "Storage usage measurements"):
		if locale == "ar" {
			return "قياسات استهلاك التخزين غير متاحة من النظام المسؤول عن التخزين."
		}
	}
	return unknown
}
