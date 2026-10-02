// Package lifecycle provides the CMS side of the shared lifecycle-operation
// boundary. Claims coordinate existing owners; they do not grant permission to
// perform the owner effect recorded by a campaign.
package lifecycle

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	ResourceTenant       = "tenant"
	ResourceLane         = "lane"
	ResourceSource       = "source"
	ResourceItem         = "item"
	ResourceArtifact     = "artifact"
	ClaimActive          = "active"
	ClaimReleased        = "released"
	PhaseAll             = "all"
	PhaseContentWrite    = "content_write"
	PhaseContentCreate   = "content_create"
	PhaseSourceAdmission = "source_admission"
	PhaseSourceDispatch  = "source_dispatch"
	PhaseFeedMembership  = "feed_membership"
	PhaseFeedRecovery    = "feed_recovery"
)

var ErrConflict = errors.New("lifecycle operation conflicts with an active campaign")
var ErrIntakePaused = errors.New("source intake is held by an active lifecycle pause")

// Resource uses a canonical slash-delimited key. Source keys are
// "<lane>/<source-id>", item keys are "<lane>/<source-id-or-dash>/<item-id>",
// and artifact keys append an owner and artifact identifier to an item key.
type Resource struct {
	Type string
	Key  string
}

type Scope struct {
	TenantID string
	Lane     string
	SourceID string
	ItemID   string
}

type Claim struct {
	ID           uint       `gorm:"column:id;primaryKey"`
	PublicID     uuid.UUID  `gorm:"column:public_id;type:uuid;default:gen_random_uuid();uniqueIndex"`
	TenantID     string     `gorm:"column:tenant_id"`
	CampaignID   *uint      `gorm:"column:campaign_id"`
	Owner        string     `gorm:"column:owner"`
	ResourceType string     `gorm:"column:resource_type"`
	ResourceKey  string     `gorm:"column:resource_key"`
	Phase        string     `gorm:"column:phase"`
	State        string     `gorm:"column:state"`
	FencingToken uuid.UUID  `gorm:"column:fencing_token;type:uuid"`
	Generation   int64      `gorm:"column:generation"`
	LeaseUntil   *time.Time `gorm:"column:lease_until"`
	ReleasedAt   *time.Time `gorm:"column:released_at"`
	CreatedAt    time.Time  `gorm:"column:created_at"`
	UpdatedAt    time.Time  `gorm:"column:updated_at"`
}

type IntakePause struct {
	ID              uint       `gorm:"column:id;primaryKey"`
	PublicID        uuid.UUID  `gorm:"column:public_id;type:uuid;default:gen_random_uuid();uniqueIndex"`
	TenantID        string     `gorm:"column:tenant_id"`
	CampaignID      uint       `gorm:"column:campaign_id"`
	Lane            string     `gorm:"column:lane"`
	ContentSourceID *uuid.UUID `gorm:"column:content_source_id"`
	Owner           string     `gorm:"column:owner"`
	Reason          string     `gorm:"column:reason"`
	State           string     `gorm:"column:state"`
	FencingToken    uuid.UUID  `gorm:"column:fencing_token"`
	Generation      int64      `gorm:"column:generation"`
	CreatedAt       time.Time  `gorm:"column:created_at"`
	ReleasedAt      *time.Time `gorm:"column:released_at"`
	UpdatedAt       time.Time  `gorm:"column:updated_at"`
}

func (IntakePause) TableName() string { return "content_reset_intake_pauses" }

type IntakePauseError struct {
	Pause IntakePause
}

func (e *IntakePauseError) Error() string {
	return fmt.Sprintf("%s: %s/%s", ErrIntakePaused, e.Pause.Lane, e.Pause.PublicID)
}

func (e *IntakePauseError) Unwrap() error { return ErrIntakePaused }

func (Claim) TableName() string { return "lifecycle_operation_claims" }

type ConflictError struct {
	Claim Claim
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("%s: %s %s (%s)", ErrConflict, e.Claim.ResourceType, e.Claim.ResourceKey, e.Claim.Owner)
}

func (e *ConflictError) Unwrap() error { return ErrConflict }

func IsConflict(err error) bool {
	return errors.Is(err, ErrConflict) || err != nil && strings.Contains(err.Error(), "lifecycle_operation_conflict:")
}

func IsIntakePaused(err error) bool {
	return errors.Is(err, ErrIntakePaused) || err != nil && strings.Contains(err.Error(), "content_reset_intake_paused:")
}

func normalizeScope(scope Scope) Scope {
	scope.TenantID = strings.TrimSpace(scope.TenantID)
	scope.Lane = strings.ToLower(strings.TrimSpace(scope.Lane))
	scope.SourceID = strings.ToLower(strings.TrimSpace(scope.SourceID))
	scope.ItemID = strings.ToLower(strings.TrimSpace(scope.ItemID))
	return scope
}

func ResourcesForScope(scope Scope) ([]Resource, error) {
	scope = normalizeScope(scope)
	if strings.TrimSpace(scope.TenantID) == "" {
		return nil, errors.New("lifecycle scope requires tenant_id")
	}
	lane := scope.Lane
	if lane != "news" && lane != "pods" {
		return nil, errors.New("lifecycle scope requires lane news or pods")
	}
	resource := Resource{Type: ResourceLane, Key: lane}
	sourceID := scope.SourceID
	itemID := scope.ItemID
	if sourceID != "" {
		if _, err := uuid.Parse(sourceID); err != nil {
			return nil, errors.New("lifecycle source_id must be a UUID")
		}
		resource = Resource{Type: ResourceSource, Key: lane + "/" + sourceID}
	}
	if itemID != "" {
		if _, err := uuid.Parse(itemID); err != nil {
			return nil, errors.New("lifecycle item_id must be a UUID")
		}
		resource = Resource{Type: ResourceItem, Key: lane + "/" + sourceIDOrDash(sourceID) + "/" + itemID}
	}
	return []Resource{resource}, nil
}

func sourceIDOrDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func validateResource(resource Resource) error {
	parts := strings.Split(resource.Key, "/")
	switch resource.Type {
	case ResourceTenant:
		if resource.Key != "*" {
			return errors.New("tenant resource key must be *")
		}
	case ResourceLane:
		if len(parts) != 1 || (parts[0] != "news" && parts[0] != "pods") {
			return errors.New("lane resource key must be news or pods")
		}
	case ResourceSource:
		if len(parts) != 2 || (parts[0] != "news" && parts[0] != "pods") {
			return errors.New("source resource key must contain lane and source UUID")
		}
		if _, err := uuid.Parse(parts[1]); err != nil {
			return errors.New("source resource key must contain a source UUID")
		}
	case ResourceItem:
		if len(parts) != 3 || (parts[0] != "news" && parts[0] != "pods") {
			return errors.New("item resource key must contain lane, source, and item")
		}
		if parts[1] != "-" {
			if _, err := uuid.Parse(parts[1]); err != nil {
				return errors.New("item resource key has an invalid source UUID")
			}
		}
		if _, err := uuid.Parse(parts[2]); err != nil {
			return errors.New("item resource key has an invalid item UUID")
		}
	case ResourceArtifact:
		if len(parts) < 5 || (parts[0] != "news" && parts[0] != "pods") {
			return errors.New("artifact resource key must identify lane, source, item, owner, and artifact")
		}
		if parts[1] != "-" {
			if _, err := uuid.Parse(parts[1]); err != nil {
				return errors.New("artifact resource key has an invalid source UUID")
			}
		}
		if _, err := uuid.Parse(parts[2]); err != nil {
			return errors.New("artifact resource key has an invalid item UUID")
		}
		if strings.TrimSpace(parts[3]) == "" || strings.TrimSpace(strings.Join(parts[4:], "/")) == "" {
			return errors.New("artifact resource key requires an owner and artifact identity")
		}
	default:
		return errors.New("unsupported lifecycle resource type")
	}
	return nil
}

func laneOf(resource Resource) string {
	if resource.Type == ResourceTenant {
		return ""
	}
	return strings.Split(resource.Key, "/")[0]
}

func overlaps(left, right Resource) bool {
	if left.Type == ResourceTenant || right.Type == ResourceTenant {
		return true
	}
	if laneOf(left) != laneOf(right) {
		return false
	}
	if left.Type == ResourceLane || right.Type == ResourceLane {
		return true
	}
	leftParts := strings.Split(left.Key, "/")
	rightParts := strings.Split(right.Key, "/")
	switch {
	case left.Type == ResourceSource && right.Type == ResourceSource:
		return left.Key == right.Key
	case left.Type == ResourceSource && (right.Type == ResourceItem || right.Type == ResourceArtifact):
		return leftParts[1] == rightParts[1]
	case right.Type == ResourceSource && (left.Type == ResourceItem || left.Type == ResourceArtifact):
		return rightParts[1] == leftParts[1]
	case left.Type == ResourceItem && right.Type == ResourceItem:
		return leftParts[2] == rightParts[2]
	case left.Type == ResourceItem && right.Type == ResourceArtifact:
		return leftParts[2] == rightParts[2]
	case right.Type == ResourceItem && left.Type == ResourceArtifact:
		return rightParts[2] == leftParts[2]
	default:
		return left.Type == right.Type && left.Key == right.Key
	}
}

func lockName(tenant string, resource Resource) string {
	return "content-lifecycle/v1/" + tenant + "/" + resource.Type + "/" + resource.Key
}

func appendUniqueLock(locks map[string]bool, key string, exclusive bool) {
	if current, exists := locks[key]; !exists || exclusive && !current {
		locks[key] = exclusive
	}
}

func lockAncestors(locks map[string]bool, tenant string, resource Resource, exclusive bool) {
	appendUniqueLock(locks, "content-lifecycle/v1/"+tenant+"/tenant/*", resource.Type == ResourceTenant || exclusive && resource.Type == ResourceTenant)
	if resource.Type == ResourceTenant {
		return
	}
	parts := strings.Split(resource.Key, "/")
	lane := parts[0]
	appendUniqueLock(locks, lockName(tenant, Resource{Type: ResourceLane, Key: lane}), exclusive && resource.Type == ResourceLane)
	if resource.Type == ResourceLane {
		return
	}
	sourceID := ""
	if resource.Type == ResourceSource {
		sourceID = parts[1]
	} else if len(parts) >= 3 && parts[1] != "-" {
		sourceID = parts[1]
	}
	if sourceID != "" {
		appendUniqueLock(locks, lockName(tenant, Resource{Type: ResourceSource, Key: lane + "/" + sourceID}), exclusive && resource.Type == ResourceSource)
	}
	if resource.Type == ResourceSource {
		return
	}
	if resource.Type == ResourceItem || resource.Type == ResourceArtifact {
		itemID := parts[2]
		// Item identity is independent from source attribution. This stable key
		// makes item-scoped claims fence a writer even if a legacy row has no
		// content_source_id or its attribution is being repaired.
		appendUniqueLock(locks, "content-lifecycle/v1/"+tenant+"/item-id/"+lane+"/"+itemID, exclusive && resource.Type == ResourceItem)
		appendUniqueLock(locks, lockName(tenant, Resource{Type: ResourceItem, Key: lane + "/" + sourceIDOrDash(sourceID) + "/" + itemID}), exclusive && resource.Type == ResourceItem)
		if resource.Type == ResourceArtifact {
			appendUniqueLock(locks, lockName(tenant, resource), exclusive)
		}
	}
}

func acquireLocks(tx *gorm.DB, locks map[string]bool) error {
	keys := make([]string, 0, len(locks))
	for key := range locks {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		query := "SELECT pg_advisory_xact_lock_shared(hashtextextended(?, 0))"
		if locks[key] {
			query = "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))"
		}
		var acquired bool
		if err := tx.Raw(query+" IS NULL", key).Scan(&acquired).Error; err != nil {
			return fmt.Errorf("acquire lifecycle resource lock: %w", err)
		}
	}
	return nil
}

func lockMutationScope(tx *gorm.DB, scope Scope) ([]Resource, error) {
	resources, err := ResourcesForScope(scope)
	if err != nil {
		return nil, err
	}
	locks := make(map[string]bool)
	for _, resource := range resources {
		lockAncestors(locks, scope.TenantID, resource, false)
	}
	if err := acquireLocks(tx, locks); err != nil {
		return nil, err
	}
	return resources, nil
}

func SchemaAvailable(db *gorm.DB) bool {
	return db != nil && db.Migrator().HasTable(&Claim{})
}

// Check prevents an owner mutation from crossing an active lifecycle claim.
// Call it inside the same short database transaction as the mutation. The
// shared advisory locks make claim acquisition and this check mutually ordered.
func Check(tx *gorm.DB, scope Scope, phase string) error {
	if tx == nil {
		return errors.New("lifecycle check requires a database transaction")
	}
	scope = normalizeScope(scope)
	phase = strings.ToLower(strings.TrimSpace(phase))
	resources, err := ResourcesForScope(scope)
	if err != nil {
		return err
	}
	if err := checkResources(tx, scope.TenantID, resources, phase, phase == PhaseFeedRecovery); err != nil {
		return err
	}
	if phase == PhaseSourceAdmission || phase == PhaseSourceDispatch || phase == PhaseContentCreate {
		return checkIntakePause(tx, scope, resources[0])
	}
	return nil
}

// CheckResources checks a complete, deterministically locked resource set in
// one transaction. Owners with multi-item manifests should use this instead
// of checking rows in caller order, so claim acquisition cannot deadlock them
// through differently ordered item locks.
func CheckResources(tx *gorm.DB, tenant string, resources []Resource, phase string) error {
	return checkResources(tx, tenant, resources, phase, false)
}

// CheckExclusiveResources is for short, owner-admission transactions whose
// lane-wide operation must serialize with narrower item/source claims. The
// caller must persist its durable owner lease in the same transaction.
func CheckExclusiveResources(tx *gorm.DB, tenant string, resources []Resource, phase string) error {
	return checkResources(tx, tenant, resources, phase, true)
}

func checkResources(tx *gorm.DB, tenant string, resources []Resource, phase string, exclusive bool) error {
	if tx == nil {
		return errors.New("lifecycle resource check requires a database transaction")
	}
	tenant = strings.TrimSpace(tenant)
	phase = strings.ToLower(strings.TrimSpace(phase))
	if tenant == "" || phase == "" || len(resources) == 0 {
		return errors.New("lifecycle resource check requires tenant, phase, and resources")
	}
	if err := checkMigrationOwnerAvailability(tx); err != nil {
		return err
	}
	// The explicit migration precedes claim activation. Before it is applied,
	// there cannot be an active campaign claim; keep existing CMS write paths
	// available during the documented additive-migration rollout.
	if !SchemaAvailable(tx) {
		return nil
	}
	resources = append([]Resource(nil), resources...)
	sort.Slice(resources, func(i, j int) bool {
		if resources[i].Type == resources[j].Type {
			return resources[i].Key < resources[j].Key
		}
		return resources[i].Type < resources[j].Type
	})
	locks := make(map[string]bool)
	for i, resource := range resources {
		if err := validateResource(resource); err != nil {
			return err
		}
		if i > 0 && resources[i-1] == resource {
			continue
		}
		lockAncestors(locks, tenant, resource, exclusive)
	}
	if err := acquireLocks(tx, locks); err != nil {
		return err
	}
	var claims []Claim
	if err := tx.Where("tenant_id = ? AND state = ? AND (phase = ? OR phase = ?)", tenant, ClaimActive, PhaseAll, phase).Find(&claims).Error; err != nil {
		return fmt.Errorf("read lifecycle claims: %w", err)
	}
	for _, claim := range claims {
		resource := Resource{Type: claim.ResourceType, Key: claim.ResourceKey}
		if err := validateResource(resource); err != nil {
			return fmt.Errorf("invalid persisted lifecycle claim: %w", err)
		}
		for _, requested := range resources {
			if overlaps(resource, requested) {
				return &ConflictError{Claim: claim}
			}
		}
	}
	return nil
}

// CheckTenantEffect serializes an owner operation whose exact item scope is
// selected by an external owner. The owner must call it in the short
// transaction that moves its durable request to running. Acquire uses shared
// tenant ancestry locks for narrower claims, so the exclusive tenant lock
// orders this admission against both lane-wide and item-scoped campaigns.
func CheckTenantEffect(tx *gorm.DB, tenant, phase string) error {
	if tx == nil {
		return errors.New("lifecycle tenant-effect check requires a database transaction")
	}
	tenant = strings.TrimSpace(tenant)
	phase = strings.ToLower(strings.TrimSpace(phase))
	if tenant == "" || phase == "" {
		return errors.New("lifecycle tenant-effect check requires tenant and phase")
	}
	if err := checkMigrationOwnerAvailability(tx); err != nil {
		return err
	}
	if !SchemaAvailable(tx) {
		return nil
	}
	locks := make(map[string]bool)
	lockAncestors(locks, tenant, Resource{Type: ResourceTenant, Key: "*"}, false)
	// Owner-wide effects have no stable item IDs at CMS admission. Take the
	// tenant resource exclusively so every lane/source/item claim installer
	// (which holds the tenant ancestry lock) is ordered with this check.
	appendUniqueLock(locks, lockName(tenant, Resource{Type: ResourceTenant, Key: "*"}), true)
	if err := acquireLocks(tx, locks); err != nil {
		return err
	}
	var claims []Claim
	if err := tx.Where("tenant_id = ? AND state = ?", tenant, ClaimActive).Order("id ASC").Find(&claims).Error; err != nil {
		return fmt.Errorf("read tenant lifecycle claims: %w", err)
	}
	for _, claim := range claims {
		if err := validateResource(Resource{Type: claim.ResourceType, Key: claim.ResourceKey}); err != nil {
			return fmt.Errorf("invalid persisted lifecycle claim: %w", err)
		}
		return &ConflictError{Claim: claim}
	}
	return nil
}

// checkMigrationOwnerAvailability takes a shared row lock in the same
// transaction as owner admission. The migration coordinator takes the
// exclusive row lock before measuring inflight work and changing to
// quiescing, so it cannot seal across a just-admitted lifecycle effect.
func checkMigrationOwnerAvailability(tx *gorm.DB) error {
	if tx == nil || !tx.Migrator().HasTable("database_migration_owner_control") {
		return nil
	}
	var control struct {
		State string `gorm:"column:state"`
	}
	err := tx.Table("database_migration_owner_control").
		Clauses(clause.Locking{Strength: "SHARE"}).
		Select("state").Where("singleton = TRUE").Take(&control).Error
	if err != nil {
		return fmt.Errorf("database migration owner state is unavailable: %w", err)
	}
	if control.State != "running" {
		return fmt.Errorf("database migration owner is %s; new lifecycle effects are paused", control.State)
	}
	return nil
}

func checkIntakePause(tx *gorm.DB, scope Scope, resource Resource) error {
	if !tx.Migrator().HasTable(&IntakePause{}) {
		return nil
	}
	var pauses []IntakePause
	query := tx.Where("tenant_id = ? AND lane = ? AND state = ?", scope.TenantID, scope.Lane, "active")
	if sourceID := strings.TrimSpace(scope.SourceID); sourceID != "" {
		query = query.Where("content_source_id IS NULL OR content_source_id = ?", sourceID)
	}
	if err := query.Order("created_at ASC").Find(&pauses).Error; err != nil {
		return fmt.Errorf("read lifecycle intake pauses: %w", err)
	}
	if len(pauses) > 0 {
		return &IntakePauseError{Pause: pauses[0]}
	}
	return nil
}

func intakePauseResource(pause IntakePause) Resource {
	if pause.ContentSourceID != nil {
		return Resource{Type: ResourceSource, Key: pause.Lane + "/" + pause.ContentSourceID.String()}
	}
	return Resource{Type: ResourceLane, Key: pause.Lane}
}

func rejectActiveIntakePauses(tx *gorm.DB, tenant string, campaignID uint, resources []Resource) error {
	if tx == nil || !tx.Migrator().HasTable(&IntakePause{}) {
		return nil
	}
	var pauses []IntakePause
	if err := tx.Where("tenant_id = ? AND state = ?", tenant, "active").Find(&pauses).Error; err != nil {
		return fmt.Errorf("read active lifecycle intake pauses: %w", err)
	}
	for _, pause := range pauses {
		if pause.CampaignID == campaignID {
			continue
		}
		pausedResource := intakePauseResource(pause)
		if err := validateResource(pausedResource); err != nil {
			return fmt.Errorf("invalid persisted intake pause scope: %w", err)
		}
		for _, resource := range resources {
			if overlaps(pausedResource, resource) {
				return &IntakePauseError{Pause: pause}
			}
		}
	}
	return nil
}

func rejectOtherLifecycleClaims(tx *gorm.DB, tenant string, campaignID uint, resource Resource) error {
	if tx == nil || !tx.Migrator().HasTable(&Claim{}) {
		return nil
	}
	var claims []Claim
	if err := tx.Where("tenant_id = ? AND state = ?", tenant, ClaimActive).Find(&claims).Error; err != nil {
		return fmt.Errorf("read active lifecycle claims: %w", err)
	}
	for _, claim := range claims {
		claimedResource := Resource{Type: claim.ResourceType, Key: claim.ResourceKey}
		if err := validateResource(claimedResource); err != nil {
			return fmt.Errorf("invalid persisted lifecycle claim: %w", err)
		}
		if claim.CampaignID != nil && *claim.CampaignID == campaignID {
			continue
		}
		if overlaps(resource, claimedResource) {
			return &ConflictError{Claim: claim}
		}
	}
	return nil
}

// AcquireIntakePause creates a restart-stable admission pause for one lane or
// source. It refuses to take ownership while provider-effecting source work is
// active; that work must be settled by its owner first.
func AcquireIntakePause(tx *gorm.DB, scope Scope, campaignID uint, owner, reason string) (IntakePause, error) {
	if tx == nil || !tx.Migrator().HasTable(&IntakePause{}) {
		return IntakePause{}, errors.New("lifecycle intake-pause schema is unavailable")
	}
	if err := checkMigrationOwnerAvailability(tx); err != nil {
		return IntakePause{}, err
	}
	if strings.TrimSpace(owner) == "" || strings.TrimSpace(reason) == "" || campaignID == 0 {
		return IntakePause{}, errors.New("intake pause requires campaign, owner, and reason")
	}
	owner = strings.TrimSpace(owner)
	reason = strings.TrimSpace(reason)
	scope = normalizeScope(scope)
	resources, err := ResourcesForScope(scope)
	if err != nil {
		return IntakePause{}, err
	}
	resource := resources[0]
	if resource.Type != ResourceLane && resource.Type != ResourceSource {
		return IntakePause{}, errors.New("intake pauses can be scoped only to a lane or source")
	}
	locks := make(map[string]bool)
	lockAncestors(locks, scope.TenantID, resource, true)
	if err := acquireLocks(tx, locks); err != nil {
		return IntakePause{}, err
	}
	if err := rejectOtherLifecycleClaims(tx, scope.TenantID, campaignID, resource); err != nil {
		return IntakePause{}, err
	}
	if resource.Type == ResourceSource {
		var category string
		if err := tx.Table("content_sources").Select("category").Where("tenant_id = ? AND public_id = ?", scope.TenantID, resourceKeySourceID(resource.Key)).Take(&category).Error; err != nil {
			return IntakePause{}, errors.New("intake pause source is not present in this tenant")
		}
		if (scope.Lane == "news" && category != "news") || (scope.Lane == "pods" && category != "media") {
			return IntakePause{}, errors.New("intake pause source does not belong to the requested lane")
		}
	}
	if err := rejectActiveSourceRunEffects(tx, scope.TenantID, resources); err != nil {
		return IntakePause{}, err
	}
	var existing IntakePause
	query := tx.Where("tenant_id = ? AND campaign_id = ? AND lane = ? AND state = ?", scope.TenantID, campaignID, scope.Lane, "active")
	if resource.Type == ResourceSource {
		query = query.Where("content_source_id = ?", resourceKeySourceID(resource.Key))
	} else {
		query = query.Where("content_source_id IS NULL")
	}
	if err := query.First(&existing).Error; err == nil {
		if existing.Owner != owner || existing.Reason != reason {
			return IntakePause{}, errors.New("campaign already owns this intake pause with different intent")
		}
		return existing, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return IntakePause{}, err
	}
	var generation int64
	if err := tx.Model(&IntakePause{}).Where("tenant_id = ? AND lane = ? AND content_source_id IS NOT DISTINCT FROM ?", scope.TenantID, scope.Lane, sourceIDPointer(resource)).Select("COALESCE(MAX(generation),0)").Scan(&generation).Error; err != nil {
		return IntakePause{}, err
	}
	pause := IntakePause{
		TenantID: scope.TenantID, CampaignID: campaignID, Lane: scope.Lane,
		ContentSourceID: sourceIDPointer(resource), Owner: owner, Reason: reason,
		State: "active", FencingToken: uuid.New(), Generation: generation + 1,
	}
	if err := tx.Create(&pause).Error; err != nil {
		return IntakePause{}, err
	}
	return pause, nil
}

func resourceKeySourceID(key string) string {
	parts := strings.Split(key, "/")
	if len(parts) != 2 {
		return ""
	}
	return parts[1]
}

func sourceIDPointer(resource Resource) *uuid.UUID {
	if resource.Type != ResourceSource {
		return nil
	}
	id, err := uuid.Parse(resourceKeySourceID(resource.Key))
	if err != nil {
		return nil
	}
	return &id
}

func ReleaseIntakePause(tx *gorm.DB, tenant string, campaignID uint, publicID, fencingToken uuid.UUID) error {
	if tx == nil || publicID == uuid.Nil || fencingToken == uuid.Nil {
		return errors.New("intake pause release requires its public ID and fencing token")
	}
	var pause IntakePause
	if err := tx.Where("tenant_id = ? AND campaign_id = ? AND public_id = ?", tenant, campaignID, publicID).First(&pause).Error; err != nil {
		return err
	}
	if pause.FencingToken != fencingToken {
		return errors.New("intake pause release fence does not match")
	}
	resource := Resource{Type: ResourceLane, Key: pause.Lane}
	if pause.ContentSourceID != nil {
		resource = Resource{Type: ResourceSource, Key: pause.Lane + "/" + pause.ContentSourceID.String()}
	}
	locks := make(map[string]bool)
	lockAncestors(locks, tenant, resource, true)
	if err := acquireLocks(tx, locks); err != nil {
		return err
	}
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND campaign_id = ? AND public_id = ?", tenant, campaignID, publicID).First(&pause).Error; err != nil {
		return err
	}
	if pause.FencingToken != fencingToken {
		return errors.New("intake pause release fence does not match")
	}
	if pause.State == "released" {
		return nil
	}
	now := time.Now().UTC()
	result := tx.Model(&IntakePause{}).Where("tenant_id = ? AND campaign_id = ? AND public_id = ? AND fencing_token = ? AND state = ?", tenant, campaignID, publicID, fencingToken, "active").Updates(map[string]any{"state": "released", "released_at": now, "updated_at": now})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errors.New("intake pause release lost its active fence")
	}
	return nil
}

// Acquire installs a sorted, fenced set of resource claims. It must run in the
// same transaction that admits the approved campaign phase. Existing effect
// handlers still retain their own qualification gates.
func Acquire(tx *gorm.DB, tenant string, campaignID uint, owner, phase string, resources []Resource) ([]Claim, error) {
	if tx == nil || !tx.Migrator().HasTable(&Claim{}) {
		return nil, errors.New("lifecycle claim schema is unavailable")
	}
	if err := checkMigrationOwnerAvailability(tx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(tenant) == "" || campaignID == 0 || strings.TrimSpace(owner) == "" || strings.TrimSpace(phase) == "" {
		return nil, errors.New("lifecycle claim requires tenant, campaign, owner, and phase")
	}
	tenant = strings.TrimSpace(tenant)
	owner = strings.TrimSpace(owner)
	phase = strings.ToLower(strings.TrimSpace(phase))
	resources = append([]Resource(nil), resources...)
	if len(resources) == 0 {
		return nil, errors.New("lifecycle claim requires at least one resource")
	}
	sort.Slice(resources, func(i, j int) bool {
		if resources[i].Type == resources[j].Type {
			return resources[i].Key < resources[j].Key
		}
		return resources[i].Type < resources[j].Type
	})
	for i, resource := range resources {
		if err := validateResource(resource); err != nil {
			return nil, err
		}
		if i > 0 && resources[i-1] == resource {
			return nil, errors.New("duplicate lifecycle resource claim")
		}
	}
	locks := make(map[string]bool)
	for _, resource := range resources {
		lockAncestors(locks, tenant, resource, true)
	}
	if err := acquireLocks(tx, locks); err != nil {
		return nil, err
	}
	if err := rejectActiveIntakePauses(tx, tenant, campaignID, resources); err != nil {
		return nil, err
	}
	if phase == PhaseAll || phase == PhaseFeedRecovery {
		if err := rejectActiveFeedRecoveryLeases(tx, tenant, resources); err != nil {
			return nil, err
		}
	}
	if phase == PhaseAll || phase == PhaseSourceAdmission || phase == PhaseSourceDispatch {
		if err := rejectActiveSourceRunEffects(tx, tenant, resources); err != nil {
			return nil, err
		}
	}
	if phase == PhaseAll || phase == PhaseContentWrite {
		if err := rejectActiveContentStageEffects(tx, tenant, resources); err != nil {
			return nil, err
		}
		if err := rejectActiveContentOwnerEffects(tx, tenant, resources); err != nil {
			return nil, err
		}
		if err := rejectActiveStorageOperationSagas(tx, tenant, resources); err != nil {
			return nil, err
		}
	}
	if err := rejectActivePodsResetRuns(tx, tenant, resources); err != nil {
		return nil, err
	}
	if err := rejectActiveRetentionOwnerRequests(tx, tenant); err != nil {
		return nil, err
	}
	var existing []Claim
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND state = ?", tenant, ClaimActive).Find(&existing).Error; err != nil {
		return nil, fmt.Errorf("read active lifecycle claims: %w", err)
	}
	for _, resource := range resources {
		for _, claim := range existing {
			if err := validateResource(Resource{Type: claim.ResourceType, Key: claim.ResourceKey}); err != nil {
				return nil, fmt.Errorf("invalid persisted lifecycle claim: %w", err)
			}
			if claim.CampaignID != nil && *claim.CampaignID == campaignID {
				if claim.ResourceType == resource.Type && claim.ResourceKey == resource.Key {
					if claim.Owner != owner || claim.Phase != phase {
						return nil, errors.New("campaign already holds this lifecycle resource under a different owner or phase")
					}
					continue
				}
				if overlaps(resource, Resource{Type: claim.ResourceType, Key: claim.ResourceKey}) {
					continue
				}
			}
			if overlaps(resource, Resource{Type: claim.ResourceType, Key: claim.ResourceKey}) {
				return nil, &ConflictError{Claim: claim}
			}
		}
	}
	claims := make([]Claim, 0, len(resources))
	for _, resource := range resources {
		var existingClaim Claim
		if err := tx.Where("tenant_id = ? AND campaign_id = ? AND resource_type = ? AND resource_key = ? AND state = ?", tenant, campaignID, resource.Type, resource.Key, ClaimActive).First(&existingClaim).Error; err == nil {
			claims = append(claims, existingClaim)
			continue
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
		var generation int64
		if err := tx.Model(&Claim{}).Where("tenant_id = ? AND resource_type = ? AND resource_key = ?", tenant, resource.Type, resource.Key).Select("COALESCE(MAX(generation),0)").Scan(&generation).Error; err != nil {
			return nil, err
		}
		claim := Claim{
			TenantID: tenant, CampaignID: &campaignID, Owner: owner,
			ResourceType: resource.Type, ResourceKey: resource.Key, Phase: phase,
			State: ClaimActive, FencingToken: uuid.New(), Generation: generation + 1,
		}
		if err := tx.Create(&claim).Error; err != nil {
			return nil, fmt.Errorf("persist lifecycle claim: %w", err)
		}
		claims = append(claims, claim)
	}
	return claims, nil
}

func rejectActivePodsResetRuns(tx *gorm.DB, tenant string, resources []Resource) error {
	if !tx.Migrator().HasTable("pods_reset_runs") {
		return nil
	}
	for _, resource := range resources {
		if resource.Type != ResourceTenant && laneOf(resource) != "pods" {
			continue
		}
		var count int64
		if err := tx.Table("pods_reset_runs").
			Where("tenant_id = ? AND ((state = ? AND expires_at > ?) OR state IN ?)",
				tenant, "approved", time.Now().UTC(), []string{"executing", "partial"}).
			Count(&count).Error; err != nil {
			return fmt.Errorf("read active Plan 119 Pods Reset runs: %w", err)
		}
		if count > 0 {
			return errors.New("approved or resumable Plan 119 Pods Reset runs conflict with lifecycle claim")
		}
		break
	}
	return nil
}

func rejectActiveRetentionOwnerRequests(tx *gorm.DB, tenant string) error {
	if !tx.Migrator().HasTable("retention_owner_requests") {
		return nil
	}
	var count int64
	if err := tx.Table("retention_owner_requests").
		Where("tenant_id = ? AND ((status = ? AND expires_at > ?) OR status IN ?)",
			tenant, "approved", time.Now().UTC(), []string{"running", "submitted", "outcome_unknown"}).
		Count(&count).Error; err != nil {
		return fmt.Errorf("read active Retention owner requests: %w", err)
	}
	if count > 0 {
		return errors.New("active Retention owner effects conflict with lifecycle claim")
	}
	return nil
}

func rejectActiveFeedRecoveryLeases(tx *gorm.DB, tenant string, resources []Resource) error {
	if !tx.Migrator().HasTable("feed_recovery_lane_leases") {
		return nil
	}
	for _, resource := range resources {
		if resource.Type == ResourceTenant {
			var count int64
			if err := tx.Table("feed_recovery_lane_leases").Where("tenant_id = ? AND expires_at > ?", tenant, time.Now().UTC()).Count(&count).Error; err != nil {
				return err
			}
			if count > 0 {
				return errors.New("active feed recovery run conflicts with lifecycle claim")
			}
			continue
		}
		if resource.Type != ResourceLane && resource.Type != ResourceSource && resource.Type != ResourceItem && resource.Type != ResourceArtifact {
			continue
		}
		lane := laneOf(resource)
		if lane == "pods" {
			lane = "media"
		}
		var count int64
		if err := tx.Table("feed_recovery_lane_leases").Where("tenant_id = ? AND lane = ? AND expires_at > ?", tenant, lane, time.Now().UTC()).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return fmt.Errorf("active feed recovery run on %s conflicts with lifecycle claim", lane)
		}
	}
	return nil
}

func rejectActiveSourceRunEffects(tx *gorm.DB, tenant string, resources []Resource) error {
	if !tx.Migrator().HasTable("content_sources") {
		return nil
	}
	if tx.Migrator().HasTable("source_run_requests") {
		for _, resource := range resources {
			query := tx.Table("source_run_requests AS request").
				Joins("JOIN content_sources AS source ON source.public_id = request.content_source_id AND source.tenant_id = request.tenant_id").
				Where("request.tenant_id = ? AND request.state IN ?", tenant, []string{"requested", "accepted", "running", "verification_required"})
			switch resource.Type {
			case ResourceTenant:
			case ResourceLane:
				category := resource.Key
				if category == "pods" {
					category = "media"
				}
				query = query.Where("source.category = ?", category)
			case ResourceSource:
				query = query.Where("request.content_source_id = ?", resourceKeySourceID(resource.Key))
			default:
				continue
			}
			var count int64
			if err := query.Count(&count).Error; err != nil {
				return err
			}
			if count > 0 {
				return fmt.Errorf("active source-run requests conflict with lifecycle %s claim", resource.Type)
			}
		}
	}
	if !tx.Migrator().HasTable("source_run_attempts") {
		return nil
	}
	activeStates := []string{"authorized", "claimed", "running", "verification_required"}
	for _, resource := range resources {
		query := tx.Table("source_run_attempts AS attempt").
			Joins("JOIN content_sources AS source ON source.public_id = attempt.content_source_id AND source.tenant_id = attempt.tenant_id").
			Where("attempt.tenant_id = ? AND attempt.state IN ?", tenant, activeStates)
		switch resource.Type {
		case ResourceTenant:
		case ResourceLane:
			category := resource.Key
			if category == "pods" {
				category = "media"
			}
			query = query.Where("source.category = ?", category)
		case ResourceSource:
			parts := strings.Split(resource.Key, "/")
			query = query.Where("attempt.content_source_id = ?", parts[1])
		default:
			continue
		}
		var count int64
		if err := query.Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return fmt.Errorf("active source-run effects conflict with lifecycle %s claim", resource.Type)
		}
	}
	return nil
}

func rejectActiveContentStageEffects(tx *gorm.DB, tenant string, resources []Resource) error {
	if !tx.Migrator().HasTable("content_stage_requests") || !tx.Migrator().HasTable("content_items") {
		return nil
	}
	activeStates := []string{"claimed", "running", "verifying", "uncertain", "reconciling"}
	for _, resource := range resources {
		query := tx.Table("content_stage_requests AS stage").
			Joins("JOIN content_items AS item ON item.public_id=stage.content_item_id AND item.tenant_id=stage.tenant_id").
			Where("stage.tenant_id=? AND stage.state IN ?", tenant, activeStates)
		switch resource.Type {
		case ResourceTenant:
		case ResourceLane:
			query = query.Where("stage.lane=?", resource.Key)
		case ResourceSource:
			query = query.Where("item.content_source_id=?", resourceKeySourceID(resource.Key))
		case ResourceItem, ResourceArtifact:
			parts := strings.Split(resource.Key, "/")
			query = query.Where("stage.content_item_id=?", parts[2])
		default:
			continue
		}
		var count int64
		if err := query.Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return fmt.Errorf("active content-stage effects conflict with lifecycle %s claim", resource.Type)
		}
	}
	return nil
}

func rejectActiveContentOwnerEffects(tx *gorm.DB, tenant string, resources []Resource) error {
	contentOwners := []struct {
		table, contentColumn, stateColumn string
		states                            []string
	}{
		{"transcription_jobs", "content_item_id", "status", []string{"queued", "running", "writeback_failed"}},
		{"transcription_batch_items", "content_item_id", "status", []string{"pending", "accepted"}},
		{"transcription_generations", "content_item_id", "state", []string{"queued", "claimed", "running", "verifying", "uncertain"}},
		{"atomization_generations", "parent_content_item_id", "state", []string{"queued", "claimed", "running", "verifying", "uncertain"}},
		{"media_rendition_generations", "content_item_id", "state", []string{"planning", "running", "verifying", "uncertain"}},
		{"pipeline_repair_requests", "content_item_id", "state", []string{"awaiting_approval", "queued", "claimed", "running", "verifying", "uncertain"}},
		{"pipeline_stage_leases", "content_item_id", "state", []string{"claimed", "running", "verifying", "unknown"}},
		{"artifact_coverage_requests", "content_item_id", "state", []string{"queued", "claimed", "running", "verifying", "uncertain"}},
		{"atomization_work_requests", "parent_content_item_id", "state", []string{"queued", "claimed", "running", "verifying", "uncertain"}},
		{"media_atomization_runs", "parent_content_item_id", "status", []string{"queued", "processing", "running"}},
	}
	for _, resource := range resources {
		for _, owner := range contentOwners {
			if !tx.Migrator().HasTable(owner.table) {
				continue
			}
			query := tx.Table(owner.table+" AS owner").
				Joins("JOIN content_items AS item ON item.public_id=owner."+owner.contentColumn+" AND item.tenant_id=owner.tenant_id").
				Where("owner.tenant_id=? AND owner."+owner.stateColumn+" IN ?", tenant, owner.states)
			query = scopeContentOwnerQuery(query, resource)
			var count int64
			if err := query.Count(&count).Error; err != nil {
				return fmt.Errorf("read active %s owner work: %w", owner.table, err)
			}
			if count > 0 {
				return fmt.Errorf("active %s work conflicts with lifecycle %s claim", owner.table, resource.Type)
			}
		}
		unitOwners := []struct {
			table, generationTable, itemColumn string
			states                             []string
		}{
			{"transcription_segment_units", "transcription_generations", "content_item_id", []string{"claimed", "running", "verifying", "uncertain"}},
			{"atomization_chapter_units", "atomization_generations", "parent_content_item_id", []string{"claimed", "running", "verifying", "uncertain"}},
		}
		for _, owner := range unitOwners {
			if !tx.Migrator().HasTable(owner.table) || !tx.Migrator().HasTable(owner.generationTable) {
				continue
			}
			query := tx.Table(owner.table+" AS unit").
				Joins("JOIN "+owner.generationTable+" AS generation ON generation.public_id=unit.generation_id AND generation.tenant_id=unit.tenant_id").
				Joins("JOIN content_items AS item ON item.public_id=generation."+owner.itemColumn+" AND item.tenant_id=generation.tenant_id").
				Where("unit.tenant_id=? AND unit.state IN ?", tenant, owner.states)
			query = scopeContentOwnerQuery(query, resource)
			var count int64
			if err := query.Count(&count).Error; err != nil {
				return fmt.Errorf("read active %s work: %w", owner.table, err)
			}
			if count > 0 {
				return fmt.Errorf("active %s work conflicts with lifecycle %s claim", owner.table, resource.Type)
			}
		}
		if tx.Migrator().HasTable("studio_clearance_requests") && tx.Migrator().HasTable("atomization_work_requests") {
			query := tx.Table("studio_clearance_requests AS request").
				Where("request.tenant_id=? AND request.state IN ?", tenant, []string{"queued", "claimed", "running", "verifying", "uncertain"})
			if resource.Type == ResourceTenant {
				// A tenant claim intersects the exact child set regardless of lane.
			}
			childPredicate, childArgs := studioClearanceScopePredicate(resource)
			if childPredicate != "" {
				query = query.Where(`(
					EXISTS (
						SELECT 1
						FROM jsonb_array_elements_text(COALESCE(request.child_ids, '[]'::jsonb)) AS child(id)
						JOIN content_items AS item ON item.public_id::text=child.id AND item.tenant_id=request.tenant_id
						WHERE `+childPredicate+`
					) OR EXISTS (
						SELECT 1
						FROM atomization_work_requests AS work
						JOIN content_items AS item ON item.public_id=work.parent_content_item_id AND item.tenant_id=work.tenant_id
						WHERE work.public_id=request.atomization_request_id AND work.tenant_id=request.tenant_id AND `+childPredicate+`
					)
				)`, append(childArgs, childArgs...)...)
			}
			var count int64
			if err := query.Count(&count).Error; err != nil {
				return fmt.Errorf("read active Studio clearance work: %w", err)
			}
			if count > 0 {
				return fmt.Errorf("active Studio clearance work conflicts with lifecycle %s claim", resource.Type)
			}
		}
		if tx.Migrator().HasTable("media_supply_action_requests") && (resource.Type == ResourceTenant || laneOf(resource) == "pods") {
			query := tx.Table("media_supply_action_requests AS action").
				Where("action.tenant_id=? AND action.state IN ?", tenant, []string{"awaiting_approval", "queued", "claimed", "running", "verifying", "uncertain"})
			if resource.Type != ResourceTenant {
				query = query.Where(`(
					(action.target_type='content_item' AND action.target_id IN (
						SELECT item.public_id FROM content_items AS item WHERE item.tenant_id=action.tenant_id AND `+scopeContentItemPredicate(resource)+`
					)) OR (action.target_type='atomization_work_request' AND action.target_id IN (
						SELECT work.public_id FROM atomization_work_requests AS work JOIN content_items AS item ON item.public_id=work.parent_content_item_id AND item.tenant_id=work.tenant_id WHERE work.tenant_id=action.tenant_id AND `+scopeContentItemPredicate(resource)+`
					))
				)`, scopeContentItemArgs(resource)...)
			}
			var count int64
			if err := query.Count(&count).Error; err != nil {
				return fmt.Errorf("read active media supply actions: %w", err)
			}
			if count > 0 {
				return fmt.Errorf("active media supply action conflicts with lifecycle %s claim", resource.Type)
			}
		}
	}
	return nil
}

func scopeContentOwnerQuery(query *gorm.DB, resource Resource) *gorm.DB {
	switch resource.Type {
	case ResourceTenant:
		return query
	case ResourceLane:
		lane := resource.Key
		if lane == "pods" {
			lane = "media"
		}
		return query.Where("item.type IN ?", laneContentTypes(lane))
	case ResourceSource:
		return query.Where("item.content_source_id=?", resourceKeySourceID(resource.Key))
	case ResourceItem, ResourceArtifact:
		parts := strings.Split(resource.Key, "/")
		return query.Where("item.public_id=?", parts[2])
	default:
		return query.Where("1=0")
	}
}

func scopeContentItemPredicate(resource Resource) string {
	switch resource.Type {
	case ResourceTenant:
		return "TRUE"
	case ResourceLane:
		return "item.type IN ?"
	case ResourceSource:
		return "item.content_source_id=?"
	case ResourceItem, ResourceArtifact:
		return "item.public_id=?"
	default:
		return "FALSE"
	}
}

func scopeContentItemArgs(resource Resource) []any {
	switch resource.Type {
	case ResourceLane:
		lane := resource.Key
		if lane == "pods" {
			lane = "media"
		}
		return []any{laneContentTypes(lane)}
	case ResourceSource:
		return []any{resourceKeySourceID(resource.Key)}
	case ResourceItem, ResourceArtifact:
		return []any{strings.Split(resource.Key, "/")[2]}
	default:
		return nil
	}
}

func studioClearanceScopePredicate(resource Resource) (string, []any) {
	if resource.Type == ResourceTenant {
		return "TRUE", nil
	}
	return scopeContentItemPredicate(resource), scopeContentItemArgs(resource)
}

func rejectActiveStorageOperationSagas(tx *gorm.DB, tenant string, resources []Resource) error {
	if !tx.Migrator().HasTable("storage_operation_sagas") {
		return nil
	}
	for _, resource := range resources {
		query := tx.Table("storage_operation_sagas AS saga").
			Where("saga.tenant_id=? AND saga.state NOT IN ?", tenant, []string{"cms_committed", "cancelled", "failed"})
		switch resource.Type {
		case ResourceTenant:
		case ResourceLane:
			lane := resource.Key
			if lane == "pods" {
				lane = "media"
			}
			query = query.Joins("JOIN content_items AS item ON item.public_id=saga.content_item_id AND item.tenant_id=saga.tenant_id").
				Where("item.type IN ?", laneContentTypes(lane))
		case ResourceSource:
			query = query.Joins("JOIN content_items AS item ON item.public_id=saga.content_item_id AND item.tenant_id=saga.tenant_id").
				Where("item.content_source_id=?", resourceKeySourceID(resource.Key))
		case ResourceItem, ResourceArtifact:
			parts := strings.Split(resource.Key, "/")
			query = query.Where("saga.content_item_id=?", parts[2])
		default:
			continue
		}
		var count int64
		if err := query.Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return fmt.Errorf("unresolved Storage owner sagas conflict with lifecycle %s claim", resource.Type)
		}
	}
	return nil
}

func laneContentTypes(lane string) []string {
	if lane == "media" {
		return []string{"VIDEO", "PODCAST"}
	}
	return []string{"NEWS", "ARTICLE", "TWEET", "COMMENT"}
}

// Release requires the exact fence tokens returned from Acquire. Expired
// worker leases are deliberately not enough to release durable intent.
func Release(tx *gorm.DB, tenant string, campaignID uint, tokens map[uuid.UUID]uuid.UUID) error {
	tenant = strings.TrimSpace(tenant)
	if len(tokens) == 0 {
		return errors.New("lifecycle release requires at least one fencing token")
	}
	var claims []Claim
	if err := tx.Where("tenant_id = ? AND campaign_id = ? AND state = ?", tenant, campaignID, ClaimActive).Find(&claims).Error; err != nil {
		return err
	}
	locks := make(map[string]bool)
	for _, claim := range claims {
		lockAncestors(locks, tenant, Resource{Type: claim.ResourceType, Key: claim.ResourceKey}, true)
	}
	if err := acquireLocks(tx, locks); err != nil {
		return err
	}
	now := time.Now().UTC()
	for publicID, token := range tokens {
		result := tx.Model(&Claim{}).Where("tenant_id = ? AND campaign_id = ? AND public_id = ? AND fencing_token = ? AND state = ?", tenant, campaignID, publicID, token, ClaimActive).Updates(map[string]any{"state": ClaimReleased, "released_at": now, "updated_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("lifecycle release fence did not match an active claim")
		}
	}
	return nil
}
