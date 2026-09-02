package controllers

import "content-management-system/src/supply"

// Register the exact anonymous Pods assembly path for the Supply supervisor.
// This is an in-process, tenant-explicit read; it creates no session, does not
// consult seen state or preferences, and never records serve telemetry.
func init() {
	supply.RegisterPodsReturnProbe(buildPodsSupplyReturnProbe)
}
