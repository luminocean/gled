package exp

// Ex is an expression for data query
type Ex interface {
	IsExpression()
}

type ComparisonEx struct {
	left  OpValue
	op    OpCode
	right OpValue
}

func (ex ComparisonEx) IsExpression() {}

type OrEx struct {
	Exps []Ex
}

func (ex OrEx) IsExpression() {}

type AndEx struct {
	Exps []Ex
}

func (ex AndEx) IsExpression() {}
