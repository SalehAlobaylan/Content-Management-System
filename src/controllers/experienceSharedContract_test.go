package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestExperienceSharedContract(t *testing.T) {
	raw, err := os.ReadFile("../../../fixtures/real-experience-contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		SchemaVersion    int `json:"schema_version"`
		Phases, Outcomes []string
		Cases            []struct {
			Name     string
			Accepted bool
			Event    incomingRuxEvent
			Metrics  map[string][2]int64
		}
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.SchemaVersion != models.RuxSchemaVersion {
		t.Fatal("schema drift")
	}
	for _, registry := range []struct {
		expected []string
		actual   map[string]bool
	}{{fixture.Phases, models.RuxPhases}, {fixture.Outcomes, models.RuxOutcomes}} {
		if len(registry.expected) != len(registry.actual) {
			t.Fatal("enum drift")
		}
		for _, value := range registry.expected {
			if !registry.actual[value] {
				t.Fatalf("missing enum %s", value)
			}
		}
	}
	now, _ := time.Parse(time.RFC3339, "2026-10-13T00:00:00Z")
	for _, example := range fixture.Cases {
		for _, platform := range []string{"web", "ios", "android"} {
			t.Run(platform+"/"+example.Name, func(t *testing.T) {
				incoming := example.Event
				if platform != "web" {
					incoming.SchemaVersion = 3
					incoming.Client.Platform = platform
					incoming.Client.OSMajor = 26
					incoming.Client.BrowserFamily = "other"
					incoming.Client.BrowserMajor = 0
				}
				event, ok := validateAndNormalizeRuxEvent(&incoming, now)
				if ok != example.Accepted {
					t.Fatalf("admission=%v expected=%v", ok, example.Accepted)
				}
				if !ok {
					return
				}
				event.TerminalStatus = "accepted"
				metrics := map[string][2]int64{}
				for _, contribution := range contributionsFor(&event) {
					metrics[contribution.metric] = [2]int64{contribution.num, contribution.denom}
				}
				if !reflect.DeepEqual(metrics, example.Metrics) {
					t.Fatalf("populations drifted: got%v expected%v", metrics, example.Metrics)
				}
			})
		}
	}
}
