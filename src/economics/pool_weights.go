package economics

import (
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"sort"
	"strings"
)

type poolKey struct{ Expense, Rule string }
type poolWeightStore struct{ old, changed map[poolKey]int64 }

// Removed identity cannot shrink a denominator and increase everybody else's share.
func (p *poolWeightStore) Preserve(c Cost, w map[string]int64) error {
	var total int64
	for _, n := range w {
		if n < 0 || total > 1e15-n {
			return fmt.Errorf("content pool exceeds measured capacity")
		}
		total += n
	}
	key := poolKey{c.SourceID + ":" + c.LogicalKey, RuleVersion + "/" + c.AppliedRule}
	if old := p.old[key]; old > total {
		w["removed"] += old - total
		total = old
	}
	if total > p.old[key] {
		p.old[key], p.changed[key] = total, total
	}
	return nil
}
func (p *poolWeightStore) Save(tx *gorm.DB, tenant string) error {
	if len(p.changed) == 0 {
		return nil
	}
	var rows []struct {
		TenantID, ExpenseKey, RuleVersion string
		Weight                            int64
	}
	for k, w := range p.changed {
		rows = append(rows, struct {
			TenantID, ExpenseKey, RuleVersion string
			Weight                            int64
		}{tenant, k.Expense, k.Rule, w})
	}
	return tx.Table("economics_pool_weights").Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "expense_key"}, {Name: "rule_version"}}, DoUpdates: clause.AssignmentColumns([]string{"weight"})}).CreateInBatches(rows, 500).Error
}
func appliedProfiles(c Cost, profiles []Profile) string {
	parts := []string{}
	seen := map[string]bool{}
	for _, p := range profiles {
		if p.Resource == c.Resource && !p.EffectiveFrom.After(c.PeriodStart) && !seen[p.Operation] {
			seen[p.Operation] = true
			parts = append(parts, p.Operation+":"+fmt.Sprint(p.Version))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
