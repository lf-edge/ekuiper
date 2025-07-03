package api

type Tuple struct {
	StreamName   string
	Columns      *Message
	AffiliateRow *Message
	Meta         map[string]string
}

func (t *Tuple) ValueByKey(s string) (*Datum, bool) {
	v, ok := t.Columns.ValueByKey(s)
	if ok {
		return v, true
	}
	v, ok = t.AffiliateRow.ValueByKey(s)
	if ok {
		return v, true
	}
	return nil, false
}

func (t *Tuple) ValueByColumnIndex(i int) (*Datum, bool) {
	return t.Columns.ValueByIndex(i)
}

func (t *Tuple) ValueByAffiliateRowIndex(i int) (*Datum, bool) {
	return t.AffiliateRow.ValueByIndex(i)
}

func (t *Tuple) ToMap() map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range t.Columns.ToMap() {
		result[k] = v
	}
	for k, v := range t.AffiliateRow.ToMap() {
		result[k] = v
	}
	return result
}

func FromMapToTuple(m map[string]interface{}) (*Tuple, error) {
	cm, err := MapToMessage(m)
	if err != nil {
		return nil, err
	}
	return &Tuple{Columns: cm}, nil
}

type Message struct {
	ColumnNames []string
	Values      []*Datum
}

func (t *Message) ValueByKey(s string) (*Datum, bool) {
	for index, name := range t.ColumnNames {
		if name == s {
			return t.Values[index], true
		}
	}
	return nil, false
}

func (t *Message) ValueByIndex(i int) (*Datum, bool) {
	if i >= 0 && i < len(t.Values) {
		return t.Values[i], true
	}
	return nil, false
}

func (t *Message) ToMap() map[string]interface{} {
	result := make(map[string]interface{})
	for i, col := range t.ColumnNames {
		if i < len(t.Values) {
			result[col] = datumToInterface(t.Values[i])
		}
	}
	return result
}

func MapToMessage(m map[string]interface{}) (*Message, error) {
	columnNames := make([]string, 0, len(m))
	values := make([]*Datum, 0, len(m))
	for col, val := range m {
		columnNames = append(columnNames, col)
		v, err := interfaceToDatum(val)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return &Message{ColumnNames: columnNames, Values: values}, nil
}
