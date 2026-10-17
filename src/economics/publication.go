package economics

type summaryIdentity struct {
	Grain                                                     string
	Day                                                       string
	Subject, Resource, Feature, Source, Expense, Method, Rule string
}

func identity(s Summary) summaryIdentity {
	return summaryIdentity{s.Grain, s.Day.UTC().Format("2006-01-02"), s.Subject, s.Resource, s.Feature, s.SourceID, s.ExpenseKey, s.Method, s.RuleVersion}
}
func groupSummaries(rows []Summary) map[summaryIdentity]Summary {
	out := map[summaryIdentity]Summary{}
	for _, s := range rows {
		k := identity(s)
		old, ok := out[k]
		if !ok {
			out[k] = s
			continue
		}
		old.Direct = Add(old.Direct, s.Direct)
		old.Shared = Add(old.Shared, s.Shared)
		if old.BeforeBenefits != nil && s.BeforeBenefits != nil {
			a := Add(*old.BeforeBenefits, *s.BeforeBenefits)
			old.BeforeBenefits = &a
		} else {
			old.BeforeBenefits = nil
		}
		old.Reconciled = old.Reconciled && s.Reconciled
		if s.ExpenseRevision > old.ExpenseRevision {
			old.ExpenseRevision = s.ExpenseRevision
		}
		out[k] = old
	}
	return out
}
