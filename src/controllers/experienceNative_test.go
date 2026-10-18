package controllers

import (
	"content-management-system/src/models"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestExperienceNativeTicketExpiryForgeryAndPurpose(t *testing.T) {
	now := time.Now().UTC()
	ticket := nativeExperienceTicket{TenantID: "native-tenant", Platform: "ios", Session: uuid.NewString(), Page: uuid.NewString(), Release: "ios-build-1", Expires: now.Add(30 * time.Minute).Unix()}
	value := issueNativeTicket(ticket, "private-fixture")
	if got, ok := readNativeTicket(value, "private-fixture", now); !ok || got != ticket {
		t.Fatal("valid bounded ticket refused")
	}
	for _, example := range []struct {
		value, key string
		at         time.Time
	}{{value, "different", now}, {value + "a", "private-fixture", now}, {value, "private-fixture", now.Add(31 * time.Minute)}, {value, "", now}} {
		if _, ok := readNativeTicket(example.value, example.key, example.at); ok {
			t.Fatal("forged/expired/unconfigured ticket accepted")
		}
	}
}
func TestExperienceNativeContractDoesNotPretendToBeWeb(t *testing.T) {
	in := validRuxEvent()
	in.SchemaVersion = 3
	in.Client.Platform = "ios"
	in.Client.OSMajor = 26
	out, ok := validateAndNormalizeRuxEvent(&in, time.Now().UTC())
	if !ok || out.ClientPlatform != "ios" || out.OSMajor != 26 {
		t.Fatal("native platform dropped")
	}
	in.Client.Platform = "web"
	if _, ok := validateAndNormalizeRuxEvent(&in, time.Now()); ok {
		t.Fatal("native schema accepted Web")
	}
	p := models.DefaultExperiencePolicy("default")
	if experienceClientEnabled(p, "ios") {
		t.Fatal("migration/default silently enabled native")
	}
	p.EnabledClients = "web,ios"
	if !experienceClientEnabled(p, "ios") || experienceClientEnabled(p, "android") {
		t.Fatal("client allowlist broadened")
	}
}
